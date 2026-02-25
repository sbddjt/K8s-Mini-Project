import os
import json
from typing import Optional, Dict, Any
from redis import Redis
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

# Redis 클라이언트
redis_client = Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    db=int(os.getenv("REDIS_DB", 0)),
    password=os.getenv("REDIS_PASSWORD", ""),
    decode_responses=True
    health_check_interval=30
)

class VehicleTelemetry(BaseModel):
    """차량 텔레메트리 데이터 모델"""
    vehicle_id: str
    vin: str
    model: str
    driver: str
    timestamp: str
    location: Dict[str, Any]
    telemetry: Dict[str, Any]
    status: Dict[str, Any]
    diagnostics: Dict[str, Any]
    events: list

def get_vehicle_telemetry(vehicle_id: str) -> Optional[VehicleTelemetry]:
    """개별 차량 최신 데이터 조회"""
    key = f"vehicle:{vehicle_id}:latest"
    data_str = redis_client.get(key)
    if data_str:
        data = json.loads(data_str)
        return VehicleTelemetry(**data)
    return None
