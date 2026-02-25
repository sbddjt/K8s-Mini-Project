from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
import json

# DB 및 카프카 연동을 위한 라이브러리
from pymongo import MongoClient
import redis
from kafka import KafkaProducer

app = FastAPI(title = "Connected Car Ingest API")

# 1. 인프라 연결 설정 (Mongo DB, Redis, Kafka)
try:
    # MongoDB: 전체 이력 저장
    mongo_client = MongoClient("mongodb://localhost:27017/")
    history_col = mongo_client["car_db"]["telemetry_history"]

    # Redis: 최신 상태 캐싱
    redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

    # Kafka: 실시간 스트리밍
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
except Exception as e:
    print(f"인프라 연결 실패 (확인 필요): {e}")

# 2. 데이터 규격 정의 (Pydantic 모델)
# 이 구조와 다르게 들어오는 JSON은 FastAPI가 자동으로 거절

class VehicleInfo(BaseModel):
    vehicle_id: str
    vin: str
    model: str
    driver: str
    timestamp_utc: str

class Coordinates(BaseModel):
    latitude: float
    longitude: float

class LocationInfo(BaseModel):
    city: str
    coordinates: Coordinates
    heading_deg: float
    altitude_m: float
    gps_accuracy_m: float

class TripInfo(BaseModel):
    state: str
    duration_s: int
    duration_hms: str
    speed_kmh: float
    odometer_km: float
    odometer_delta_km: float

class BatteryInfo(BaseModel):
    soc_pct: float
    health_pct: float
    pack_voltage_v: float
    pack_current_a: float
    aux_12v_battery_v: float
    is_charging: bool

class ConnectedCarData(BaseModel):
    # 최상위 데이터 모델
    vehicle: VehicleInfo
    location: LocationInfo
    trip: TripInfo
    battery: BatteryInfo
    temperatures_c: Dict[str, float]
    dynamics: Dict[str, Any]
    status: Dict[str, Any]
    diagnostics: Dict[str, Any]
    events: List[str]

# 3. API 엔드포인트
@app.post("/api/query/telemetry")
async def ingest_telemetry(data: ConnectedCarData):
    # 차량 데이터를 수신하여 유효성 검사 후 Mongo, Redis, Kafka로 분배

    # 0. 데이터 파싱 (Dict로 바꿔줌)
    payload = data.model_dump()
    v_id = data.vehicle.vehicle_id

    try:
        # 1. Redis: 최신 상태 저장
        redis_client.set(f"car:latest:{v_id}", json.dumps(payload))

        # 2. Kafka: 핵심 정보 필터링 전송
        kafka_payload = {
            "vehicle_id": v_id,
            "timestamp": data.vehicle.timestamp_utc,
            "state": data.trip.state,
            "speed": data.trip.speed_kmh,
            "soc": data.battery.soc_pct,
            "location": payload["location"]["coordinates"],
            "recent_event": data.events[-1] if data.events else None
        }
        producer.send('car-live-updates', value=kafka_payload)

        # 3. MongoDB: 이력 저장
        history_col.insert_one(payload)

        return {"status": "success", "processed_vehicle": v_id}
    
    except Exception as e:
        print(f"Error processing telemetry: {e}")
        raise HTTPException(status_code=500, detail="서버 내부 처리 중 오류가 발생했습니다.")
