from fastapi import FastAPI, Request
from pymongo import MongoClient
import redis
import json

app = FastAPI(title= "Connected Car Command API")

# -------------------------------------------------------------------
# DB 연결 설정


# 1. MongoDB 연결
mongo_client = MongoClient("mongodb://localhost:27017/")
db = mongo_client["car_db"] # 데이터베이스 이름
collection = db["car_logs"] # 테이블(컬렉션) 이름

# 2. Redis 연결
redis_client = redis.Redis(host = 'localhost', port = 6379, db = 0, decode_responses = True)


# POST API: 차량 데이터 수신 엔드포인트
@app.post("/api/v1/telemetry")
async def receive_telemetry(request: Request):
    # 1. 시뮬레이터가 보낸 JSON 데이터 읽기
    data = await request.json()
    car_id = data.get("car_id")

    # 2. [Command] MongoDB에 데이터 원본 그대로 무조건 들이붓기 (Insert)
    collection.insert_one(data.copy())

    # 3. [Query용 동기화] Redis에 가장 최신 상태 덮어쓰기 (Set)
    redis_key = f"car:latest:{car_id}"
    redis_client.set(redis_key, json.dumps(data))

    # 4. 시뮬레이터에게 "잘 받았다"고 응답
    return {
        "status": "success",
        "message": f"Data saved for {car_id}",
        "mongo_inserted": True,
        "redis_updated": True
    }
