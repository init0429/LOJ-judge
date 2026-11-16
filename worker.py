import redis
import io
import json
import os
import socket
import tarfile
import time
import threading
import logging
from requests.exceptions import ReadTimeout, ConnectionError as RequestsConnectionError
from urllib3.exceptions import ReadTimeoutError
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


# 소스코드와 입력은 tar 묶음으로 만들어 컨테이너 stdin에 흘려보내고 /tmp에 풀어서 씀.
# 명령행 인자로 넘기면 리눅스의 인자 하나당 128KB 제한(MAX_ARG_STRLEN)에 걸려서
# 큰 코드나 큰 입력은 컨테이너가 아예 뜨지 못함.
BUNDLE_FAILED = "__LOJ_BUNDLE_FAILED__"
UNPACK = f"tar -xf - -C /tmp || {{ echo {BUNDLE_FAILED} >&2; exit 125; }}"
STDIN_FILE = ".stdin"


def _bundle(files: dict) -> bytes:
    """{파일명: bytes}를 샌드박스 사용자(1000) 소유의 tar 묶음으로 만든다."""
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w", format=tarfile.USTAR_FORMAT) as archive:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            info.mode = 0o755
            info.uid = info.gid = 1000
            archive.addfile(info, io.BytesIO(data))
    return buffer.getvalue()


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


def _run_container(cfg: dict, command: str, timeout_seconds: float, files: dict):
    """
    공통 컨테이너 실행 헬퍼. (exit_code, logs, timed_out) 반환
    files는 실행 전에 /tmp에 풀리는 {파일명: bytes} (command가 UNPACK으로 시작해야 함)
    """
    container = None
    stream = None
    stdin = None
    reader = None
    feeder = None
    payload = _bundle(files)
    output = [bytearray(), bytearray()]
    failures = []
    overflow = threading.Event()
    limit = 1024 * 1024
    try:
        container = client.containers.create(
            image=cfg["image"],
            command=["sh", "-c", command],
            # stdin을 붙이려면 detach=False여야 AttachStdin/StdinOnce가 켜짐
            detach=False,
            stdin_open=True,
            network_disabled=True,
            mem_limit=cfg["mem_limit"],
            # 동시 채점끼리 CPU를 나눠 쓰며 실행 시간이 흔들리지 않도록 1코어로 고정
            nano_cpus=1_000_000_000,
            pids_limit=50,
            user="1000:1000",
            cap_drop=["ALL"],
            security_opt=["no-new-privileges"],
            read_only=True,
            tmpfs={"/tmp": "rw,exec,nosuid,size=64m,mode=1777"},
            log_config=docker.types.LogConfig(type="none"),
            working_dir="/tmp",
        )
        # Attach before starting so short programs cannot outrun output capture.
        stream = container.attach(stdout=True, stderr=True, stream=True, demux=True)
        stdin = container.attach_socket(params={"stdin": 1, "stream": 1})

        def feed():
            try:
                raw = getattr(stdin, "_sock", stdin)
                raw.sendall(payload)
                raw.shutdown(socket.SHUT_WR)
            except OSError:
                # 묶음을 다 읽기 전에 컨테이너가 끝났거나 강제 종료된 경우.
                # 전달 실패 여부는 아래에서 종료 코드(BUNDLE_FAILED)로 판별함.
                pass

        def capture():
            try:
                for stdout, stderr in stream:
                    for index, chunk in enumerate((stdout, stderr)):
                        if not chunk:
                            continue
                        remaining = limit - sum(map(len, output))
                        output[index].extend(chunk[:remaining])
                        if len(chunk) > remaining:
                            overflow.set()
                            try:
                                container.kill()
                            except docker.errors.APIError as exc:
                                if exc.status_code != 409:
                                    raise
                            return
            except Exception as exc:
                failures.append(exc)

        container.start()
        reader = threading.Thread(target=capture, daemon=True)
        reader.start()
        feeder = threading.Thread(target=feed, daemon=True)
        feeder.start()

        try:
            result = container.wait(timeout=timeout_seconds)
        except (ReadTimeout, RequestsConnectionError) as exc:
            if not isinstance(exc, ReadTimeout) and not any(
                isinstance(arg, ReadTimeoutError) for arg in exc.args
            ):
                raise
            try:
                container.kill()
            except Exception:
                pass
            return None, "", True

        reader.join(timeout=5)
        if reader.is_alive():
            raise RuntimeError("Sandbox output reader did not finish")
        if failures:
            raise RuntimeError("Sandbox output capture failed") from failures[0]
        if overflow.is_set():
            return 1, "Output limit exceeded", False
        exit_code = result.get("StatusCode", -1)
        if exit_code == 125 and BUNDLE_FAILED.encode() in output[1]:
            raise RuntimeError("Sandbox bundle delivery failed")
        raw = output[0] if exit_code == 0 else output[0] + output[1]
        logs = raw.decode("utf-8", errors="replace").strip()
        return exit_code, logs, False
    finally:
        if container is not None:
            try:
                container.remove(force=True)
            except Exception:
                pass
        for attached in (stream, stdin):
            if attached is not None:
                try:
                    attached.close()
                except OSError:
                    pass  # 컨테이너가 먼저 끝나 이미 닫힌 소켓
                except Exception:
                    logging.exception("Failed to close sandbox stream")
        for thread in (reader, feeder):
            if thread is not None:
                thread.join(timeout=5)


def compile_check(sub_id: str, cfg: dict, code: str):
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

    command = f"{UNPACK} && {cfg['compile']}"
    files = {cfg["filename"]: code.encode("utf-8")}

    exit_code, logs, timed_out = _run_container(cfg, command, 10.0, files)

    if timed_out:
        print(f"[{sub_id}] 컴파일 시간 초과")
        return False, "컴파일 시간 초과"

    if exit_code != 0:
        print(f"[{sub_id}] 컴파일 실패:\n{logs[:4096]}")
        return False, logs

    return True, ""


def run_one_testcase(sub_id: str, cfg: dict, code: str, tc: dict):
    """
    테스트케이스 하나를 컨테이너에서 실행하고 결과를 반환
    반환값: (status, actual_output)
    status: "Accepted" | "Wrong Answer" | "Time Limit Exceeded" | "Runtime Error" | "System Error"
    """
    expected_output = tc["output"]
    files = {
        cfg["filename"]: code.encode("utf-8"),
        STDIN_FILE: (tc["input"] or "").encode("utf-8"),
    }

    steps = [UNPACK]
    if cfg["compile"]:
        steps.append(f"{cfg['compile']} 1>&2")
    steps.append(f"{cfg['run']} < /tmp/{STDIN_FILE}")
    command = " && ".join(steps)

    try:
        exit_code, logs, timed_out = _run_container(cfg, command, cfg["timeout"], files)
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
        print(f"[{sub_id}] Testcase: 런타임 에러 (exit code {exit_code})\n{logs[:4096]}")
        return "Runtime Error", logs

    if logs == expected_output.strip():
        print(f"[{sub_id}] Testcase: 통과 ✓")
        return "Accepted", logs
    else:
        print(f"[{sub_id}] Testcase: 오답 (Wrong Answer)")
        return "Wrong Answer", logs


def process_submission(task: dict):
    try:
        if task.get("is_test") and task.get("expires_at", float("inf")) <= time.time():
            return
        _process_submission(task)
    except Exception:
        logging.exception("Submission processing failed: %s", task.get("submission_id"))
        sub_id = task.get("submission_id")
        if sub_id is None:
            raise
        if task.get("is_test", False):
            r.publish(f"test_result:{sub_id}", json.dumps({
                "status": "System Error", "test_cases": [],
            }))
        else:
            update_status(sub_id, "System Error")


def _process_submission(task: dict):
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

    ok, message = compile_check(sub_id, cfg, code)
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
        if is_test and task.get("expires_at", float("inf")) <= time.time():
            return
        status, actual = run_one_testcase(sub_id, cfg, code, tc)

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
            if _future.exception() is not None:
                logging.error("Judge task failed", exc_info=(
                    type(_future.exception()), _future.exception(),
                    _future.exception().__traceback__,
                ))
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
