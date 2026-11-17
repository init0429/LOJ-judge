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
from datetime import datetime, timezone
from typing import NamedTuple, Optional

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
# - artifact: 컴파일 결과물 파일명. 컴파일은 제출당 한 번만 하고, 이 파일 하나를
#   테스트케이스마다 실행 컨테이너로 넘겨 재사용함
# - run: 실행 명령 (표준입력은 파이프로 붙음). {mem}은 문제의 메모리 제한(MB)으로 바뀜
# - time_factor / time_extra: 문제의 시간 제한에 곱하고 더하는 언어별 보정.
#   실행 시간 = 제한 × time_factor + time_extra(초). 인터프리터와 JVM은 기동과 실행이
#   느리므로 같은 풀이가 언어 때문에 시간 초과되지 않게 여유를 줌
# - mem_extra: 런타임 자체가 쓰는 메모리(MB). 컨테이너 제한 = 문제 제한 + mem_extra
NATIVE = {"time_factor": 1, "time_extra": 0.0, "mem_extra": 16}
SCRIPT = {"time_factor": 3, "time_extra": 2.0, "mem_extra": 32}

LANGUAGES = {
    "javascript": {
        "image": "node:24-alpine",
        "filename": "main.js",
        "compile": None,
        "run": "node --max-old-space-size={mem} /tmp/main.js",
        **SCRIPT,
        "mem_extra": 128,
    },
    "typescript": {
        "image": "node:24-alpine",
        "filename": "main.ts",
        "compile": None,
        # Native type stripping supports erasable syntax, not tsc or npm packages.
        "run": "node --max-old-space-size={mem} /tmp/main.ts",
        **SCRIPT,
        "mem_extra": 128,
    },
    "ruby": {
        "image": "ruby:3.4-alpine",
        "filename": "main.rb",
        "compile": None,
        "run": "ruby /tmp/main.rb",
        **SCRIPT,
    },
    "php": {
        "image": "php:8.4-cli-alpine",
        "filename": "main.php",
        "compile": None,
        "run": "php -d memory_limit={mem}M /tmp/main.php",
        **SCRIPT,
    },
    "perl": {
        "image": "perl:5.40-slim",
        "filename": "main.pl",
        "compile": None,
        "run": "perl /tmp/main.pl",
        **SCRIPT,
    },
    "python": {
        "image": "python:3.11-alpine",
        "filename": "main.py",
        "compile": None,
        "run": "python3 /tmp/main.py",
        **SCRIPT,
    },
    "c": {
        "image": "gcc:13",
        "filename": "main.c",
        "compile": "gcc -O2 -std=c11 -o /tmp/main /tmp/main.c",
        "artifact": "main",
        "run": "/tmp/main",
        **NATIVE,
    },
    "cpp": {
        "image": "gcc:13",
        "filename": "main.cpp",
        "compile": "g++ -O2 -std=c++17 -o /tmp/main /tmp/main.cpp",
        "artifact": "main",
        "run": "/tmp/main",
        **NATIVE,
    },
    "java": {
        "image": "eclipse-temurin:17-jdk-alpine",
        "filename": "Main.java",
        # 클래스 파일이 여러 개 나올 수 있어 jar 하나로 묶어서 넘김
        "compile": (
            "mkdir /tmp/classes && javac -d /tmp/classes /tmp/Main.java"
            " && jar cf /tmp/main.jar -C /tmp/classes ."
        ),
        "artifact": "main.jar",
        "run": "java -cp /tmp/main.jar -Xmx{mem}m Main",
        # 힙(-Xmx) 밖에서 JVM이 쓰는 메타스페이스·코드 캐시·스레드 스택 몫
        "time_factor": 2,
        "time_extra": 1.0,
        "mem_extra": 128,
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
        **SCRIPT,
    },
}

# 문제에 제한이 비어 있을 때 쓰는 값과, 잘못 등록된 값이 채점 서버를 붙잡지 못하게 하는 범위
DEFAULT_TIME_LIMIT_MS = 2000
DEFAULT_MEMORY_LIMIT_MB = 128
TIME_LIMIT_RANGE_MS = (100, 30_000)
MEMORY_LIMIT_RANGE_MB = (16, 2048)
# 시간 제한을 넘겼는지는 컨테이너가 기록한 실행 시간으로 판정하고, 이만큼 더 기다린 뒤에
# 강제로 끝냄 (Docker API 왕복 시간 때문에 제한 직전에 끝난 풀이를 죽이지 않기 위함)
TIME_GRACE = 1.0


# 소스코드와 입력은 tar 묶음으로 만들어 컨테이너 stdin에 흘려보내고 /tmp에 풀어서 씀.
# 명령행 인자로 넘기면 리눅스의 인자 하나당 128KB 제한(MAX_ARG_STRLEN)에 걸려서
# 큰 코드나 큰 입력은 컨테이너가 아예 뜨지 못함.
BUNDLE_FAILED = "__LOJ_BUNDLE_FAILED__"
UNPACK = f"tar -xf - -C /tmp || {{ echo {BUNDLE_FAILED} >&2; exit 125; }}"
STDIN_FILE = ".stdin"

OUTPUT_LIMIT = 1024 * 1024
# 컴파일러는 실행 제한과 무관하게 넉넉히 줌 (g++로 <bits/stdc++.h>를 -O2 컴파일하면 수백 MB를 씀)
COMPILE_TIMEOUT = 30.0
COMPILE_MEM_MB = 512
ARTIFACT_LIMIT = 16 * 1024 * 1024
MESSAGE_LIMIT = 8192


class SandboxResult(NamedTuple):
    exit_code: Optional[int]
    stdout: bytes = b""
    stderr: bytes = b""
    timed_out: bool = False
    overflow: bool = False
    oom: bool = False
    # 컨테이너 안에서 프로그램이 돈 시간(초). 시간 초과로 끊겼으면 None
    elapsed: Optional[float] = None

    def text(self) -> str:
        """성공하면 stdout만, 실패하면 진단을 위해 stderr까지 합쳐서 돌려준다."""
        raw = self.stdout if self.exit_code == 0 else self.stdout + self.stderr
        return raw.decode("utf-8", errors="replace").strip()


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


def load_problem(problem_id, public_only: bool):
    """
    문제의 제한과 테스트케이스를 DB에서 읽는다.
    큐에는 제출 정보만 싣고 테스트 데이터는 여기서 직접 가져오므로, 테스트 데이터가
    큰 문제도 큐 용량 제한에 걸리지 않는다. public_only면 공개 예제만 읽는다(예제 실행).

    반환값: (problem: dict, test_cases: [{"input", "output"}])
    """
    problem = (
        supabase.table("problems")
        .select("time_limit_ms, memory_limit_mb")
        .eq("id", problem_id)
        .execute()
    )
    if not problem.data:
        raise LookupError(f"Problem {problem_id} does not exist")

    query = (
        supabase.table("test_cases")
        .select("input_data, expected_output")
        .eq("problem_id", problem_id)
    )
    if public_only:
        query = query.eq("is_hidden", False)
    rows = query.order("id").execute().data or []
    test_cases = [
        {"input": row["input_data"] or "", "output": row["expected_output"] or ""}
        for row in rows
    ]
    return problem.data[0], test_cases


def _clamp(value, bounds):
    return max(bounds[0], min(bounds[1], value))


def run_limits(cfg: dict, problem: dict):
    """문제의 제한에 언어별 보정을 적용한다. 반환값: (실행 시간 제한(초), 메모리 제한(MB))"""
    time_ms = _clamp(problem.get("time_limit_ms") or DEFAULT_TIME_LIMIT_MS, TIME_LIMIT_RANGE_MS)
    memory_mb = _clamp(problem.get("memory_limit_mb") or DEFAULT_MEMORY_LIMIT_MB, MEMORY_LIMIT_RANGE_MB)
    return time_ms / 1000 * cfg["time_factor"] + cfg["time_extra"], int(memory_mb)


def _docker_time(value: str) -> float:
    """Docker가 기록한 RFC 3339 시각(나노초 단위)을 유닉스 초로 바꾼다."""
    whole, _, fraction = value.rstrip("Z").partition(".")
    seconds = datetime.strptime(whole, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc).timestamp()
    return seconds + (float(f"0.{fraction}") if fraction else 0.0)


def _run_stats(container, fallback: float):
    """
    끝난 컨테이너의 (실행 시간, OOM 여부)를 읽는다.
    Docker가 남긴 시작/종료 시각을 쓰면 API 호출이 오가는 시간이 실행 시간에 섞이지
    않는다. 읽지 못하면 워커가 잰 시간(fallback)으로 대신한다.
    """
    try:
        container.reload()
        state = container.attrs["State"]
        elapsed = _docker_time(state["FinishedAt"]) - _docker_time(state["StartedAt"])
        return (elapsed if elapsed >= 0 else fallback), bool(state.get("OOMKilled"))
    except Exception:
        return fallback, False


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


def _run_container(image: str, command: str, files: dict, *, mem_mb: int,
                   timeout: float, output_limit: int = OUTPUT_LIMIT) -> SandboxResult:
    """
    공통 컨테이너 실행 헬퍼.
    files는 실행 전에 /tmp에 풀리는 {파일명: bytes} (command가 UNPACK으로 시작해야 함)
    """
    container = None
    stream = None
    stdin = None
    stdin_socket = None
    reader = None
    feeder = None
    payload = _bundle(files)
    output = [bytearray(), bytearray()]
    failures = []
    overflow = threading.Event()
    try:
        container = client.containers.create(
            image=image,
            command=["sh", "-c", command],
            # stdin을 붙이려면 detach=False여야 AttachStdin/StdinOnce가 켜짐
            detach=False,
            stdin_open=True,
            network_disabled=True,
            mem_limit=f"{mem_mb}m",
            # 스왑을 허용하면 메모리 제한을 넘겨 쓸 수 있으므로 같은 값으로 막음
            memswap_limit=f"{mem_mb}m",
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
        # docker-py는 읽기 전용 래퍼를 돌려주므로 쓰기는 안쪽의 실제 소켓으로 함
        stdin_socket = getattr(stdin, "_sock", stdin)

        def feed():
            try:
                stdin_socket.sendall(payload)
                stdin_socket.shutdown(socket.SHUT_WR)
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
                        remaining = output_limit - sum(map(len, output))
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
        started = time.monotonic()
        reader = threading.Thread(target=capture, daemon=True)
        reader.start()
        feeder = threading.Thread(target=feed, daemon=True)
        feeder.start()

        try:
            result = container.wait(timeout=timeout)
        except (ReadTimeout, RequestsConnectionError) as exc:
            if not isinstance(exc, ReadTimeout) and not any(
                isinstance(arg, ReadTimeoutError) for arg in exc.args
            ):
                raise
            try:
                container.kill()
            except Exception:
                pass
            return SandboxResult(None, timed_out=True)

        elapsed, oom = _run_stats(container, fallback=time.monotonic() - started)
        reader.join(timeout=5)
        if reader.is_alive():
            raise RuntimeError("Sandbox output reader did not finish")
        if failures:
            raise RuntimeError("Sandbox output capture failed") from failures[0]
        stdout, stderr = bytes(output[0]), bytes(output[1])
        if overflow.is_set():
            return SandboxResult(1, stdout, stderr, overflow=True, elapsed=elapsed)
        exit_code = result.get("StatusCode", -1)
        if exit_code == 125 and BUNDLE_FAILED.encode() in stderr:
            raise RuntimeError("Sandbox bundle delivery failed")
        return SandboxResult(exit_code, stdout, stderr, oom=oom, elapsed=elapsed)
    finally:
        if container is not None:
            try:
                container.remove(force=True)
            except Exception:
                pass
        # 래퍼를 닫아도 안쪽 소켓은 열린 채 남으므로 stdin_socket도 직접 닫음
        for attached in (stream, stdin, stdin_socket):
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


def build_program(sub_id: str, cfg: dict, code: str):
    """
    실행 컨테이너에 넣을 프로그램 파일을 준비한다.
    인터프리터 언어는 소스 그대로, 컴파일 언어는 여기서 딱 한 번 컴파일해서 나온
    결과물(artifact)을 돌려준다. 테스트케이스마다 다시 컴파일하지 않으므로
    컴파일 시간이 실행 시간 제한에 섞이지 않는다.

    반환값: (files: dict | None, message: str) — 컴파일 실패 시 files는 None
    """
    source = {cfg["filename"]: code.encode("utf-8")}
    if cfg["compile"] is None:
        return source, ""

    # 컴파일러의 stdout은 stderr로 돌리고, stdout으로는 결과물만 내보냄
    command = f"{UNPACK} && {{ {cfg['compile']}; }} 1>&2 && cat /tmp/{cfg['artifact']}"
    result = _run_container(
        cfg["image"], command, source,
        mem_mb=COMPILE_MEM_MB, timeout=COMPILE_TIMEOUT, output_limit=ARTIFACT_LIMIT,
    )

    if result.timed_out:
        print(f"[{sub_id}] 컴파일 시간 초과")
        return None, "컴파일 시간 초과"

    if result.overflow:
        print(f"[{sub_id}] 컴파일 출력 초과")
        return None, "컴파일 출력이 너무 큽니다"

    if result.exit_code != 0:
        message = result.stderr.decode("utf-8", errors="replace").strip()[:MESSAGE_LIMIT]
        print(f"[{sub_id}] 컴파일 실패:\n{message[:4096]}")
        return None, message

    return {cfg["artifact"]: result.stdout}, ""


def run_one_testcase(sub_id: str, cfg: dict, program: dict, tc: dict,
                     time_limit: float, memory_mb: int):
    """
    테스트케이스 하나를 컨테이너에서 실행하고 결과를 반환
    program은 build_program이 돌려준 파일 묶음, time_limit(초)과 memory_mb는 run_limits의 값
    반환값: (status, actual_output)
    status: "Accepted" | "Wrong Answer" | "Time Limit Exceeded" | "Runtime Error" | "System Error"
    """
    expected_output = tc["output"]
    files = {**program, STDIN_FILE: (tc["input"] or "").encode("utf-8")}
    command = f"{UNPACK} && {cfg['run'].replace('{mem}', str(memory_mb))} < /tmp/{STDIN_FILE}"
    # /tmp(tmpfs)에 푼 프로그램과 입력도 컨테이너 메모리로 잡히므로 그만큼 더 줌
    bundle_mb = -(-sum(map(len, files.values())) // (1024 * 1024))

    try:
        result = _run_container(
            cfg["image"], command, files,
            mem_mb=memory_mb + cfg["mem_extra"] + bundle_mb,
            timeout=time_limit + TIME_GRACE,
        )
    except docker.errors.ImageNotFound:
        print(f"[{sub_id}] Docker 이미지 없음: {cfg['image']}")
        return "System Error", ""
    except Exception as e:
        print(f"[{sub_id}] 시스템 에러: {str(e)}")
        return "System Error", ""

    if result.timed_out or (result.elapsed is not None and result.elapsed > time_limit):
        print(f"[{sub_id}] Testcase: 시간 초과 (Time Limit Exceeded)")
        return "Time Limit Exceeded", ""

    if result.overflow:
        print(f"[{sub_id}] Testcase: 출력 초과")
        return "Runtime Error", "Output limit exceeded"

    if result.oom:
        print(f"[{sub_id}] Testcase: 메모리 초과")
        return "Runtime Error", "Memory limit exceeded"

    logs = result.text()
    if result.exit_code != 0:
        print(f"[{sub_id}] Testcase: 런타임 에러 (exit code {result.exit_code})\n{logs[:4096]}")
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

    problem, test_cases = load_problem(task["problem_id"], public_only=is_test)

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

    program, message = build_program(sub_id, cfg, code)
    if program is None:
        if is_test:
            r.publish(f"test_result:{sub_id}", json.dumps({
                "status": "Compile Error",
                "message": message,
                "test_cases": [],
            }))
        else:
            update_status(sub_id, "Compile Error")
        return

    time_limit, memory_mb = run_limits(cfg, problem)
    test_results = []

    for idx, tc in enumerate(test_cases, 1):
        if is_test and task.get("expires_at", float("inf")) <= time.time():
            return
        status, actual = run_one_testcase(sub_id, cfg, program, tc, time_limit, memory_mb)

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
