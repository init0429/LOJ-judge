import redis
import json
import os
import time
import base64
import docker

redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
r = redis.from_url(redis_url, health_check_interval=30)

client = docker.from_env()

def process_submission(task):
  sub_id = task['submission_id']
  code = task['code']
  language = task['language']
  test_cases = task.get('test_cases', [])
  
  print(f"\n[{sub_id}] 채점 시작 (총 {len(test_cases)}개의 테스트케이스)")

  encoded_code = base64.b64encode(code.encode('utf-8')).decode('utf-8')
  
  for idx, tc in enumerate(test_cases, 1):
    test_input = tc['input']
    expected_output = tc['output']
    
    encoded_input = base64.b64encode(test_input.encode('utf-8')).decode('utf-8')
    
    if language == "python":
      image = "python:3.11-alpine"
      command = f"echo '{encoded_code}' | base64 -d > main.py && echo '{encoded_input}' | base64 -d | python3 main.py"
    else:
      print(f"[{sub_id}] ❌ 지원하지 않는 언어입니다.")
      return

    try:
      container = client.containers.run(
        image=image,
        command=["sh", "-c", command],
        detach=True,
        network_disabled=True,
        mem_limit="128m",
        pids_limit=50,
      )
      
      timeout_seconds = 2.0
      start_time = time.time()
      status = "running"
      
      while time.time() - start_time < timeout_seconds:
        container.reload()
        if container.status == "exited":
          status = "exited"
          break
        time.sleep(0.1)

      if status == "running":
        container.kill()
        print(f"[{sub_id}] Testcase #{idx}: 시간 초과 (Time Limit Exceeded)")
        container.remove()
        return
          
      result = container.wait()
      logs = container.logs().decode('utf-8')
      container.remove()

      if result['StatusCode'] != 0:
        print(f"[{sub_id}] ⚠️ Testcase #{idx}: 런타임 에러 (Runtime Error)\n{logs.strip()}")
        return
          
      if logs.strip() == expected_output.strip():
        print(f"[{sub_id}] Testcase #{idx}: 통과 (정답: {expected_output.strip()} / 출력: {logs.strip()})")
      else:
        print(f"[{sub_id}] Testcase #{idx}: 오답 (Wrong Answer)")
        print(f"   - 예상 정답: {expected_output.strip()}")
        print(f"   - 실제 출력: {logs.strip()}")
        return

    except Exception as e:
      print(f"[{sub_id}] 시스템 에러: {str(e)}")
      return

  print(f"[{sub_id}] 모든 테스트케이스 통과! (Accepted)\n")

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
      time.sleep(2)
    except Exception as e:
      print(f"워커 루프 에러: {str(e)}")
      time.sleep(1)

if __name__ == "__main__":
  main()