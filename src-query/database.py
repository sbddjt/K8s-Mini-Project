import os
import json
from typing import Optional, Dict, Any
from redis import Redis
from pydantic import BaseModel
from dotenv import load_dotenv

#load_dotenv()

# Redis 클라이언트
redis_client = Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    db=int(os.getenv("REDIS_DB", 0)),
    password=os.getenv("REDIS_PASSWORD", ""),
    decode_responses=True,
    health_check_interval=30
)

# database.py 하단에 추가 (임시)
if __name__ == "__main__":
    try:
        if redis_client.ping():
            print("✅ Redis 연결 성공!")
    except Exception as e:
        print(f"❌ Redis 연결 실패: {e}")