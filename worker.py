import redis
import json
import os
import time
import base64
import docker
from supabase import create_client, Client

url: str = os.environ.get("SUPABASE_URL", "")
key: str = os.environ.get("SUPABASE_KEY", "")
supabase: Client = create_client(url, key)

redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
r = redis.from_url(redis_url, health_check_interval=30)

client = docker.from_env()

def update_status(sub_id: str, status: str):
    try:
        supabase.table("submissions").update({"status": status}).eq("id", sub_id).execute()
    except Exception as e:
        print(f"[{sub_id}] DB 업데이트 실패: {str(e)}")

def process_submission(task: dict):
    sub_id = task["submission_id"]
    code = task["code"]
    language = task["language"]
    test_cases = task.get("test_cases", [])

    if not test_cases:
        print(f"[{sub_id}] 테스트케이스 없음. 건너뜀.")
        update_status(sub_id, "System Error")
        return

    update_status(sub_id, "Running")
    print(f"\n[{sub_id}] 채점 시작 (총 {len(test_cases)}개의 테스트케이스)")

    # base64로 인코딩하면 single quote 문제 없음
    encoded_code = base64.b64encode(code.encode("utf-8")).decode("utf-8")

    for idx, tc in enumerate(test_cases, 1):
        test_input = tc["input"]
        expected_output = tc["output"]

        encoded_input = base64.b64encode(test_input.encode("utf-8")).decode("utf-8")

        if language == "python":
            image = "python:3.11-alpine"
            # base64 디코딩 후 파일 저장 및 실행 (single quote 특수문자 문제 방지를 위해 <<EOF 방식 사용)
            command = (
                f"echo {encoded_code} | base64 -d > /tmp/main.py && "
                f"echo {encoded_input} | base64 -d | python3 /tmp/main.py"
            )
        else:
            print(f"[{sub_id}] 지원하지 않는 언어: {language}")
            update_status(sub_id, "System Error")
            return

        try:
            container = client.containers.run(
                image=image,
                command=["sh", "-c", command],
                detach=True,
                network_disabled=True,
                mem_limit="128m",
                pids_limit=50,
                user="1000:1000",
                cap_drop=["ALL"],
                working_dir="/tmp",
            )

            timeout_seconds = 5.0
            start_time = time.time()
            container_status = "running"

            while time.time() - start_time < timeout_seconds:
                container.reload()
                if container.status == "exited":
                    container_status = "exited"
                    break
                time.sleep(0.1)

            if container_status == "running":
                container.kill()
                container.remove()
                update_status(sub_id, "Time Limit Exceeded")
                print(f"[{sub_id}] Testcase #{idx}: 시간 초과 (Time Limit Exceeded)")
                return

            result = container.wait()
            logs = container.logs().decode("utf-8").strip()
            container.remove()

            exit_code = result.get("StatusCode", -1)
            if exit_code != 0:
                update_status(sub_id, "Runtime Error")
                print(f"[{sub_id}] Testcase #{idx}: 런타임 에러 (exit code {exit_code})\n{logs}")
                return

            if logs == expected_output.strip():
                print(f"[{sub_id}] Testcase #{idx}: 통과 ✓")
            else:
                update_status(sub_id, "Wrong Answer")
                print(f"[{sub_id}] Testcase #{idx}: 오답 (Wrong Answer)")
                print(f"   예상: {repr(expected_output.strip())}")
                print(f"   실제: {repr(logs)}")
                return

        except docker.errors.ImageNotFound:
            print(f"[{sub_id}] Docker 이미지 없음: {image}")
            update_status(sub_id, "System Error")
            return
        except Exception as e:
            print(f"[{sub_id}] 시스템 에러: {str(e)}")
            update_status(sub_id, "System Error")
            return

    update_status(sub_id, "Accepted")
    print(f"[{sub_id}] 모든 테스트케이스 통과! ✓ (Accepted)\n")


def main():
    print("Judge worker 가동 완료. 샌드박스 대기 중...")
    while True:
        try:
            result = r.blpop("submission_queue", timeout=2)

            if result:
                _, data = result
                task = json.loads(data.decode("utf-8"))
                process_submission(task)

        except redis.exceptions.ConnectionError:
            print("Redis 연결이 끊어졌습니다. 재연결 시도 중...")
            time.sleep(3)
        except Exception as e:
            print(f"워커 루프 에러: {str(e)}")
            time.sleep(1)


if __name__ == "__main__":
    main()