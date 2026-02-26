import json
import os
import signal
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from database import redis_client

RUNNING = True

def _handle_sigterm(signum, frame):
    global RUNNING
    RUNNING = False

signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)

def start_consumer():
    topic = os.getenv("KAFKA_TOPIC", "vehicle-events")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")  # K8s Service DNS 권장
    group_id = os.getenv("KAFKA_GROUP_ID", "vehicle-projector")
    ttl_sec = int(os.getenv("REDIS_TTL_SEC", "60"))

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap.split(","),
        group_id=group_id,
        enable_auto_commit=True,          # 간단 운영용 (정교하게 하려면 False + 수동 commit)
        auto_offset_reset="latest",       # 새 그룹이면 최신부터(관제 최신상태엔 보통 OK)
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    print(f"Kafka Consumer 시작... topic={topic}, bootstrap={bootstrap}, group={group_id}")

    try:
        while RUNNING:
            records = consumer.poll(timeout_ms=1000, max_records=100)
            for _, msgs in records.items():
                for msg in msgs:
                    try:
                        vehicle_data = msg.value
                        vehicle_id = vehicle_data.get("vehicle_id")
                        if not vehicle_id:
                            # 스키마 불일치/누락 로그
                            print(f"[WARN] missing vehicle_id: {vehicle_data}")
                            continue

                        key = f"vehicle:{vehicle_id}:latest"
                        redis_client.set(key, json.dumps(vehicle_data), ex=ttl_sec)
                        # 필요하면 최근 업데이트 차량 목록 zset도 갱신 가능
                        # redis_client.zadd("vehicles:recent", {vehicle_id: int(time.time())})

                    except Exception as e:
                        print(f"[ERROR] processing failed: {e}")

    except KafkaError as e:
        print(f"[ERROR] Kafka error: {e}")
    finally:
        consumer.close()
        print("Kafka Consumer 종료")

if __name__ == "__main__":
    start_consumer()