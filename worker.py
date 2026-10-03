import redis
import json
import os
import time
import base64
import threading
from concurrent.futures import ThreadPoolExecutor

import docker
import docker.errors
from supabase import create_client, Client

url: str = os.environ.get("SUPABASE_URL", "")
key: str = os.environ.get("SUPABASE_KEY", "")
supabase: Client = create_client(url, key)

redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
r = redis.from_url(redis_url, health_check_interval=30)

# 채점 동시 실행 개수. 컨테이너 스펙(CPU/메모리)에 맞게 조절
CONCURRENCY = int(os.getenv("JUDGE_CONCURRENCY", "4"))

# docker-py 클라이언트는 스레드 간 공유해도 안전 (매 호출이 독립적인 HTTP 요청)
client = docker.from_env()

# 언어별 설정.
# - filename: 컨테이너 안에 소스코드를 쓸 파일명 (Java는 public class명과 일치해야 함 -> Main)
# - compile: 컴파일 명령. 인터프리터 언어는 None
# - run: 실행 명령 (표준입력은 파이프로 붙음)
# - mem_limit / timeout: 언어별로 다르게 줄 필요가 있어 개별 지정 (예: JVM은 기본보다 더 필요)
LANGUAGES = {
    "javascript": {
        "image": "node:24-alpine",
        "filename": "main.js",
        "compile": None,
        "run": "node --max-old-space-size=96 /tmp/main.js",
        "mem_limit": "256m",
        "timeout": 5.0,
    },
    "typescript": {
        "image": "node:24-alpine",
        "filename": "main.ts",
        "compile": None,
        # Native type stripping supports erasable syntax, not tsc or npm packages.
        "run": "node --max-old-space-size=96 /tmp/main.ts",
        "mem_limit": "256m",
        "timeout": 5.0,
    },
    "ruby": {
        "image": "ruby:3.4-alpine",
        "filename": "main.rb",
        "compile": None,
        "run": "ruby /tmp/main.rb",
        "mem_limit": "128m",
        "timeout": 5.0,
    },
    "php": {
        "image": "php:8.4-cli-alpine",
        "filename": "main.php",
        "compile": None,
        "run": "php -d memory_limit=96M /tmp/main.php",
        "mem_limit": "128m",
        "timeout": 5.0,
    },
    "perl": {
        "image": "perl:5.40-slim",
        "filename": "main.pl",
        "compile": None,
        "run": "perl /tmp/main.pl",
        "mem_limit": "128m",
        "timeout": 5.0,
    },
    "python": {
        "image": "python:3.11-alpine",
        "filename": "main.py",
        "compile": None,
        "run": "python3 /tmp/main.py",
        "mem_limit": "128m",
        "timeout": 5.0,
    },
    "c": {
        "image": "gcc:13",
        "filename": "main.c",
        "compile": "gcc -O2 -std=c11 -o /tmp/main /tmp/main.c",
        "run": "/tmp/main",
        "mem_limit": "128m",
        "timeout": 5.0,
    },
    "cpp": {
        "image": "gcc:13",
        "filename": "main.cpp",
        "compile": "g++ -O2 -std=c++17 -o /tmp/main /tmp/main.cpp",
        "run": "/tmp/main",
        "mem_limit": "128m",
        "timeout": 5.0,
    },
    "java": {
        "image": "eclipse-temurin:17-jdk-alpine",
        "filename": "Main.java",
        "compile": "javac -d /tmp /tmp/Main.java",
        "run": "java -cp /tmp -Xmx192m Main",
        # JVM 자체 오버헤드가 있어서 python/cpp보다 여유를 더 줌.
        "mem_limit": "256m",
        "timeout": 8.0,
    },
    "sql": {
        # 전용 SQL 문제 UI(스키마/시드 데이터 입력 등)는 아직 없어서 임시로:
        # 테스트케이스의 input을 "셋업 스크립트"(CREATE TABLE/INSERT)로,
        # 학생이 낸 코드를 "쿼리 하나"로 취급해서 sqlite3 in-memory에서 실행.
        # sqlite3는 파이썬 표준 라이브러리라 별도 이미지 없이 python 이미지 재사용.
        "image": "python:3.11-alpine",
        "filename": "main.sql",
        "compile": None,
        "run": (
            "python3 -c \""
            "import sqlite3,sys;"
            "conn=sqlite3.connect(':memory:');"
            "conn.executescript(sys.stdin.read());"
            "q=open('/tmp/main.sql').read();"
            "cur=conn.cursor();"
            "cur.execute(q);"
            "[print('|'.join(str(c) for c in row)) for row in cur.fetchall()]"
            "\""
        ),
        "mem_limit": "128m",
        "timeout": 5.0,
    },
}


def update_status(sub_id: str, status: str):
    try:
        supabase.table("submissions").update({"status": status}).eq("id", sub_id).execute()
    except Exception as e:
        print(f"[{sub_id}] DB 업데이트 실패: {str(e)}")


def pull_images_on_startup():
    """
    콜드 스타트 때 첫 제출이 이미지 pull 대기로 오래 걸리는 걸 막기 위해
    기동 시점에 미리 당겨놓음. 이미 있으면 바로 스킵됨.
    """
    for lang, cfg in LANGUAGES.items():
        image = cfg["image"]
        try:
            client.images.get(image)
        except docker.errors.ImageNotFound:
            print(f"[startup] {lang} 이미지 pull 중: {image} ...")
            try:
                client.images.pull(image)
                print(f"[startup] {lang} 이미지 준비 완료: {image}")
            except Exception as e:
                print(f"[startup] {lang} 이미지 pull 실패 ({image}): {e}")


def _run_container(cfg: dict, command: str, timeout_seconds: float):
    """
    공통 컨테이너 실행 헬퍼. (exit_code, logs, timed_out) 반환
    """
    container = None
    try:
        container = client.containers.run(
            image=cfg["image"],
            command=["sh", "-c", command],
            detach=True,
            network_disabled=True,
            mem_limit=cfg["mem_limit"],
            pids_limit=50,
            user="1000:1000",
            cap_drop=["ALL"],
            working_dir="/tmp",
        )

        try:
            result = container.wait(timeout=timeout_seconds)
        except Exception:
            try:
                container.kill()
            except Exception:
                pass
            return None, "", True

        logs = container.logs().decode("utf-8", errors="replace").strip()
        exit_code = result.get("StatusCode", -1)
        return exit_code, logs, False
    finally:
        if container is not None:
            try:
                container.remove(force=True)
            except Exception:
                pass


def compile_check(sub_id: str, cfg: dict, encoded_code: str):
    """
    컴파일이 필요한 언어에 한해, 테스트케이스 루프 들어가기 전에 딱 한 번
    컴파일만 시도해서 성공 여부를 확인함. (Compile Error를 Runtime Error와
    구분하기 위한 사전 체크 — 실제 채점 컨테이너는 매번 새로 뜨기 때문에
    테스트케이스마다 다시 컴파일하긴 하지만, 여기서 이미 성공을 확인했으므로
    같은 코드가 다시 실패할 일은 없음.)

    반환값: (ok: bool, message: str)
    """
    if cfg["compile"] is None:
        return True, ""

    write_code = f"echo {encoded_code} | base64 -d > /tmp/{cfg['filename']}"
    command = f"{write_code} && {cfg['compile']}"

    exit_code, logs, timed_out = _run_container(cfg, command, timeout_seconds=10.0)

    if timed_out:
        print(f"[{sub_id}] 컴파일 시간 초과")
        return False, "컴파일 시간 초과"

    if exit_code != 0:
        print(f"[{sub_id}] 컴파일 실패:\n{logs}")
        return False, logs

    return True, ""


def run_one_testcase(sub_id: str, cfg: dict, encoded_code: str, tc: dict):
    """
    테스트케이스 하나를 컨테이너에서 실행하고 결과를 반환
    반환값: (status, actual_output)
    status: "Accepted" | "Wrong Answer" | "Time Limit Exceeded" | "Runtime Error" | "System Error"
    """
    test_input = tc["input"]
    expected_output = tc["output"]
    encoded_input = base64.b64encode(test_input.encode("utf-8")).decode("utf-8")

    write_code = f"echo {encoded_code} | base64 -d > /tmp/{cfg['filename']}"
    steps = [write_code]
    if cfg["compile"]:
        steps.append(cfg["compile"])
    steps.append(f"echo {encoded_input} | base64 -d | {cfg['run']}")
    command = " && ".join(steps)

    try:
        exit_code, logs, timed_out = _run_container(cfg, command, timeout_seconds=cfg["timeout"])
    except docker.errors.ImageNotFound:
        print(f"[{sub_id}] Docker 이미지 없음: {cfg['image']}")
        return "System Error", ""
    except Exception as e:
        print(f"[{sub_id}] 시스템 에러: {str(e)}")
        return "System Error", ""

    if timed_out:
        print(f"[{sub_id}] Testcase: 시간 초과 (Time Limit Exceeded)")
        return "Time Limit Exceeded", ""

    if exit_code != 0:
        print(f"[{sub_id}] Testcase: 런타임 에러 (exit code {exit_code})\n{logs}")
        return "Runtime Error", logs

    if logs == expected_output.strip():
        print(f"[{sub_id}] Testcase: 통과 ✓")
        return "Accepted", logs
    else:
        print(f"[{sub_id}] Testcase: 오답 (Wrong Answer)")
        return "Wrong Answer", logs


def process_submission(task: dict):
    sub_id = task["submission_id"]
    code = task["code"]
    language = task["language"]
    test_cases = task.get("test_cases", [])
    is_test = task.get("is_test", False)

    cfg = LANGUAGES.get(language)
    if cfg is None:
        print(f"[{sub_id}] 지원하지 않는 언어: {language}")
        if is_test:
            r.publish(f"test_result:{sub_id}", json.dumps({
                "status": "System Error",
                "message": f"지원하지 않는 언어: {language}",
            }))
        else:
            update_status(sub_id, "System Error")
        return

    if not test_cases:
        print(f"[{sub_id}] 테스트케이스 없음. 건너뜀.")
        if is_test:
            r.publish(f"test_result:{sub_id}", json.dumps({"status": "System Error", "message": "테스트케이스 없음"}))
        else:
            update_status(sub_id, "System Error")
        return

    if not is_test:
        update_status(sub_id, "Running")

    print(f"\n[{sub_id}] 채점 시작 ({language}, 총 {len(test_cases)}개 테스트케이스, thread={threading.current_thread().name})")

    encoded_code = base64.b64encode(code.encode("utf-8")).decode("utf-8")

    ok, message = compile_check(sub_id, cfg, encoded_code)
    if not ok:
        if is_test:
            r.publish(f"test_result:{sub_id}", json.dumps({
                "status": "Compile Error",
                "message": message,
                "test_cases": [],
            }))
        else:
            update_status(sub_id, "Compile Error")
        return

    test_results = []

    for idx, tc in enumerate(test_cases, 1):
        status, actual = run_one_testcase(sub_id, cfg, encoded_code, tc)

        test_results.append({
            "id": idx,
            "status": status,
            "input": tc["input"],
            "expected": tc["output"],
            "actual": actual,
        })

        if status != "Accepted":
            if is_test:
                # 테스트 모드는 첫 실패에서 멈추고 전체 결과를 바로 리턴
                break
            else:
                update_status(sub_id, status)
                return

    if is_test:
        overall_status = "Accepted"
        for tr in test_results:
            if tr["status"] != "Accepted":
                overall_status = tr["status"]
                break

        r.publish(f"test_result:{sub_id}", json.dumps({
            "status": overall_status,
            "test_cases": test_results
        }))
        return

    update_status(sub_id, "Accepted")
    print(f"[{sub_id}] 모든 테스트케이스 통과! ✓ (Accepted)\n")


def main():
    pull_images_on_startup()
    print(f"Judge worker 가동 완료. 동시 채점 슬롯: {CONCURRENCY}개. 지원 언어: {', '.join(LANGUAGES.keys())}. 샌드박스 대기 중...")

    with ThreadPoolExecutor(max_workers=CONCURRENCY, thread_name_prefix="judge") as pool:
        # 큐에 쌓인 만큼 무작정 스레드에 다 던지면 슬롯 이상으로 컨테이너가
        # 동시에 뜨는 걸 막기 위해, 큐에서 꺼내는 것 자체를 pool 크기만큼만 진행중이게 제한
        inflight = 0
        inflight_lock = threading.Lock()

        def on_done(_future):
            nonlocal inflight
            with inflight_lock:
                inflight -= 1

        while True:
            try:
                with inflight_lock:
                    can_pull = inflight < CONCURRENCY

                if not can_pull:
                    time.sleep(0.05)
                    continue

                result = r.blpop("submission_queue", timeout=2)

                if result:
                    _, data = result
                    task = json.loads(data.decode("utf-8"))
                    with inflight_lock:
                        inflight += 1
                    future = pool.submit(process_submission, task)
                    future.add_done_callback(on_done)

            except redis.exceptions.ConnectionError:
                print("Redis 연결이 끊어졌습니다. 재연결 시도 중...")
                time.sleep(3)
            except Exception as e:
                print(f"워커 루프 에러: {str(e)}")
                time.sleep(1)


if __name__ == "__main__":
    main()
