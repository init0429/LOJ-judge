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
  
  print(f"[{sub_id}] 샌드박스 채점 시작...")

  encoded_code = base64.b64encode(code.encode('utf-8')).decode('utf-8')
  
  if language == "python":
    image = "python:3.11-alpine"
    command = f"echo '{encoded_code}' | base64 -d | python3"
  else:
    print(f"[{sub_id}] 지원하지 않는 언어입니다.")
    return

  try:
    container = client.containers.run(
      image=image,
      command=["sh", "-c", command],
      detach=True,
      network_disabled=True,
      mem_limit="128m",
      pids_limit=50,
      # read_only=True,
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
      print(f"[{sub_id}] 시간 초과 (TLE)")
      result_output = "Time Limit Exceeded"
    else:
      result = container.wait()
      logs = container.logs().decode('utf-8').strip()
      
      if result['StatusCode'] == 0:
        print(f"[{sub_id}] 실행 성공 (출력값): {logs}")
        result_output = logs
      else:
        print(f"[{sub_id}] 런타임 에러 (RTE): {logs}")
        result_output = f"Runtime Error:\n{logs}"

    container.remove()
      
  except docker.errors.ImageNotFound:
    print(f"[{sub_id}] 도커 이미지를 찾을 수 없습니다. (먼저 docker pull {image} 필요)")
  except Exception as e:
    print(f"[{sub_id}] 시스템 에러: {str(e)}")

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