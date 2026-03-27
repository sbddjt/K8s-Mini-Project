from __future__ import annotations

import asyncio
import json
import random
import httpx
import os
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from math import asin, cos, radians, sin, sqrt
from typing import Any, Deque, Dict, List, Optional, Tuple

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# 서울 위도/경도 최소/최대
SEOUL_BOUNDS = (37.40, 37.70, 126.76, 127.20)
SEOUL_ANCHOR = ("Seoul", 37.5665, 126.9780, 55.0)

app = FastAPI(
    title="Connected Car Dummy Stream Server",
    version="0.1.0",
    description="Connected car dummy data producer for local simulation and API demo",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

EXTERNAL_BATCH_URL = os.getenv(
    "INGEST_SERVER_URL",
    "http://127.0.0.1:8000/api/telemetry/batch",
)

MAX_SPEED_KMH = 100.0
MAX_ACCEL_KMH_PER_SEC = 8.0
MAX_DECEL_KMH_PER_SEC = 25.0
INGEST_INTERVAL_MIN = float(os.getenv("INGEST_INTERVAL_MIN", "5.0"))
INGEST_INTERVAL_MAX = float(os.getenv("INGEST_INTERVAL_MAX", "10.0"))
INGEST_REQUEST_TIMEOUT = float(os.getenv("INGEST_REQUEST_TIMEOUT", "10.0"))
MAX_SEED_JITTER_DEG = 0.002
EARTH_RADIUS_KM = 6371.0088

try:
    DUMMY_SEED_COUNT = max(1, int(os.getenv("DUMMY_SEED_COUNT", "10")))
except ValueError:
    DUMMY_SEED_COUNT = 10

MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2.0
MAX_BACKOFF_DELAY = 15.0 # 딜레이 최대 15초로 설정

# ===== 하드코딩된 데이터 =====
VEHICLE_MODELS: Tuple[str, ...] = (
    "Hyundai IONIQ 5", "Hyundai IONIQ 6", "Hyundai Grandeur", "Hyundai Santa Fe",
    "Genesis GV80", "Genesis GV70", "Genesis G80", "Genesis G90",
    "Kia EV6", "Kia EV9", "Kia Sorento", "Kia K8",
    "Tesla Model Y", "Tesla Model 3", "BMW i4", "Mercedes-Benz EQS", "Porsche Taycan"
)

DRIVERS: Tuple[str, ...] = (
    "김준태", "신중훈", "조성윤", "조현준",
    "카리나", "윈터", "장원영", "안유진", "민지",
    "하니", "제니", "지수", "차은우", "정국"
)

CITY_ROUTES: Dict[str, Tuple[float, float]] = {
    "강남역": (37.4979, 127.0276),
    "여의도": (37.5219, 126.9243),
    "광화문": (37.5709, 126.9773),
    "잠실": (37.5132, 127.1000),
    "홍대": (37.5562, 126.9220),
}

ROAD_EVENTS: Tuple[str, ...] = (
    "고속도로 주행 상태가 정상입니다",
    "도심 교차로에서 정체 구간이 발생했습니다",
    "교차로에서 대기 중입니다",
    "내비게이션 경로 재탐색이 수행되었습니다",
    "차량 상태가 정상입니다",
    "사전 점검이 완료되었습니다",
    "도어 잠금 이벤트가 확인되었습니다",
    "방향지시등이 해제되었습니다",
    "제동유압 라인 점검이 완료되었습니다",
    "원격 진단이 정상입니다",
)


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    lat1_rad = radians(lat1)
    lat2_rad = radians(lat2)
    d_lat = radians(lat2 - lat1)
    d_lon = radians(lon2 - lon1)
    a = sin(d_lat / 2) ** 2 + cos(lat1_rad) * cos(lat2_rad) * sin(d_lon / 2) ** 2
    return 2 * EARTH_RADIUS_KM * asin(sqrt(a))


def _inside_seoul(lat: float, lon: float) -> bool:
    min_lat, max_lat, min_lon, max_lon = SEOUL_BOUNDS
    if not (min_lat <= lat <= max_lat and min_lon <= lon <= max_lon):
        return False
    anchor_name, anchor_lat, anchor_lon, radius_km = SEOUL_ANCHOR
    return _haversine_km(lat, lon, anchor_lat, anchor_lon) <= radius_km


def haversine_step(distance_km: float, heading_deg: float, lat: float) -> Tuple[float, float]:
    distance_m = distance_km * 1000.0
    d_lat = (distance_m * cos(radians(heading_deg))) / 111_000.0
    d_lon = (distance_m * sin(radians(heading_deg))) / (111_000.0 * max(0.2, cos(radians(lat))))
    return d_lat, d_lon


def _pick_city_route() -> Tuple[str, float, float]:
    """서울 중심에서 랜덤 위치 선택"""
    city_name, anchor_lat, anchor_lon, radius_km = SEOUL_ANCHOR
    for _ in range(20):
        heading = random.uniform(0, 360)
        distance = random.uniform(0.5, min(radius_km * 0.4, 20.0))
        d_lat, d_lon = haversine_step(distance, heading, anchor_lat)
        lat, lon = anchor_lat + d_lat, anchor_lon + d_lon
        if _inside_seoul(lat, lon):
            return city_name, lat, lon
    return city_name, anchor_lat, anchor_lon


def _normalize_seed_point(city_name: str, lat: float, lon: float) -> Tuple[str, float, float]:
    """시드 포인트에 약간의 지터 추가"""
    for _ in range(15):
        jitter_lat = lat + random.uniform(-MAX_SEED_JITTER_DEG * 4, MAX_SEED_JITTER_DEG * 4)
        jitter_lon = lon + random.uniform(-MAX_SEED_JITTER_DEG * 4, MAX_SEED_JITTER_DEG * 4)
        if _inside_seoul(jitter_lat, jitter_lon):
            return city_name, jitter_lat, jitter_lon
    # fallback
    anchor_name, anchor_lat, anchor_lon, _ = SEOUL_ANCHOR
    heading = random.uniform(0, 360)
    d_lat, d_lon = haversine_step(5.0, heading, anchor_lat)
    return anchor_name, anchor_lat + d_lat, anchor_lon + d_lon


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass
class VehicleState:
    vehicle_id: str
    vin: str
    model: str
    driver: str
    city: str
    latitude: float
    longitude: float
    odometer_km: float
    battery_soc: float
    battery_health: float
    speed_kmh: float
    trip_state: str
    is_locked: bool
    ignition_on: bool
    engine_temp_c: float
    cabin_temp_c: float
    tire_pressure_psi: Dict[str, float]
    heading_deg: float
    ambient_temp_c: float
    last_updated: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    throttle_pct: float = 0.0
    brake_pct: float = 0.0
    steering_deg: float = 0.0
    coolant_temp_c: float = field(default_factory=lambda: random.uniform(70.0, 92.0))
    is_charging: bool = False
    last_move_km: float = 0.0
    tx_buffer: Deque[Dict[str, Any]] = field(default_factory=lambda: deque(maxlen=1000))
    retry_count: int = 0
    last_retry_time: Optional[float] = None


class VehicleEnvelope(BaseModel):
    vehicle_id: str = Field(..., example="CAR-1001")
    vin: str = Field(..., example="KICF9AA1001000001")
    model: str = Field(..., example="Hyundai IONIQ 5")
    driver: str = Field(..., example="김도윤")
    timestamp: str
    location: Dict[str, Any]
    telemetry: Dict[str, Any]
    status: Dict[str, Any]
    diagnostics: Dict[str, Any]
    events: List[str]


def generate_vehicle_seeds() -> List[VehicleState]:
    """차량 시드 데이터 생성 (단순화된 버전)"""
    seeds: List[VehicleState] = []
    target_count = max(1, DUMMY_SEED_COUNT)

    for idx in range(target_count):
        # 랜덤 도시 선택
        city_name = random.choice(list(CITY_ROUTES.keys()))
        lat, lon = CITY_ROUTES[city_name]
        
        # 위치에 지터 추가
        city_name, lat, lon = _normalize_seed_point(city_name, lat, lon)
        
        vehicle_num = idx + 1
        vehicle_id = f"CAR-{1000 + vehicle_num}"
        vin = f"KICF9AA{100000000 + vehicle_num:09d}"
        
        seeds.append(
            VehicleState(
                vehicle_id=vehicle_id,
                vin=vin,
                model=random.choice(VEHICLE_MODELS),
                driver=DRIVERS[idx % len(DRIVERS)],
                city=city_name,
                latitude=lat,
                longitude=lon,
                odometer_km=random.uniform(5_000, 60_000),
                battery_soc=random.uniform(45.0, 99.0),
                battery_health=random.uniform(82.0, 100.0),
                speed_kmh=0.0,
                trip_state=random.choice(["PARK", "IDLE"]),
                is_locked=True,
                ignition_on=False,
                engine_temp_c=random.uniform(45.0, 58.0),
                cabin_temp_c=random.uniform(21.0, 26.0),
                tire_pressure_psi={
                    "front_left": random.uniform(33.0, 36.0),
                    "front_right": random.uniform(33.0, 36.0),
                    "rear_left": random.uniform(33.0, 36.0),
                    "rear_right": random.uniform(33.0, 36.0),
                },
                heading_deg=random.uniform(0, 360),
                ambient_temp_c=random.uniform(-2.0, 32.0),
            )
        )
    return seeds


def _clamp(val: float, min_v: float, max_v: float) -> float:
    return max(min_v, min(max_v, val))


def _move_within_area(vehicle: VehicleState, move_km: float) -> Tuple[float, float]:
    d_lat, d_lon = haversine_step(move_km, vehicle.heading_deg, vehicle.latitude)
    next_lat = vehicle.latitude + d_lat
    next_lon = vehicle.longitude + d_lon

    if _inside_seoul(next_lat, next_lon):
        return next_lat, next_lon

    # 바운더리 탈출 시 반대 방향으로 튕겨내기
    vehicle.heading_deg = (vehicle.heading_deg + 180.0 + random.uniform(-35.0, 35.0)) % 360
    d_lat, d_lon = haversine_step(move_km, vehicle.heading_deg, vehicle.latitude)
    next_lat = vehicle.latitude + d_lat
    next_lon = vehicle.longitude + d_lon
    if _inside_seoul(next_lat, next_lon):
        return next_lat, next_lon

    # 그래도 안되면 현재 위치 유지
    return vehicle.latitude, vehicle.longitude


def simulate_trip_state(vehicle: VehicleState, dt: float) -> None:
    p = random.random()
    if vehicle.trip_state == "PARK":
        if p < 0.05:
            vehicle.trip_state = "IDLE"
            vehicle.is_locked = True
            vehicle.ignition_on = False
        elif p < 0.09:
            vehicle.trip_state = "CHARGE"
            vehicle.is_charging = True
    elif vehicle.trip_state == "IDLE":
        if p < 0.06 and vehicle.battery_soc > 5:
            vehicle.trip_state = "DRIVE"
            vehicle.is_locked = False
            vehicle.ignition_on = True
            vehicle.speed_kmh = max(vehicle.speed_kmh, 20.0)
        elif p < 0.09:
            vehicle.trip_state = "PARK"
            vehicle.is_locked = True
            vehicle.ignition_on = False
        elif p < 0.10:
            vehicle.trip_state = "CHARGE"
            vehicle.is_charging = True
    elif vehicle.trip_state == "CHARGE":
        if p < 0.15:
            vehicle.trip_state = "IDLE"
            vehicle.is_charging = False
        elif p < 0.35:
            vehicle.battery_soc = _clamp(vehicle.battery_soc + 0.08 * dt, 0, 100)
    else:
        if p < 0.04:
            vehicle.trip_state = "IDLE"
            vehicle.ignition_on = False
            vehicle.throttle_pct = 0.0
        elif p < 0.06:
            vehicle.speed_kmh = 0.0


def update_telemetry(vehicle: VehicleState, dt: float) -> List[str]:
    events = [random.choice(ROAD_EVENTS)]
    if vehicle.trip_state == "DRIVE":
        acc = random.uniform(-2.0, 4.5)
        vehicle.throttle_pct = _clamp(vehicle.throttle_pct + acc * 0.9 + random.uniform(-1.5, 1.5), 0, 100)
        vehicle.brake_pct = max(0.0, 12.0 - acc + random.uniform(-2.0, 5.0))
        desired_speed = _clamp(55.0 + (vehicle.throttle_pct / 100.0) * 45.0, 0.0, MAX_SPEED_KMH)
        max_up = MAX_ACCEL_KMH_PER_SEC * dt
        max_down = MAX_DECEL_KMH_PER_SEC * dt
        speed_delta = _clamp(desired_speed - vehicle.speed_kmh, -max_down, max_up)
        vehicle.speed_kmh = _clamp(vehicle.speed_kmh + speed_delta, 0.0, MAX_SPEED_KMH)
        move_km = vehicle.speed_kmh * dt / 3600.0
        prev_lat, prev_lon = vehicle.latitude, vehicle.longitude
        next_lat, next_lon = _move_within_area(vehicle, move_km)
        vehicle.latitude = next_lat
        vehicle.longitude = next_lon
        if next_lat == prev_lat and next_lon == prev_lon:
            vehicle.last_move_km = 0.0
        else:
            vehicle.last_move_km = move_km
            vehicle.odometer_km += vehicle.last_move_km
        vehicle.battery_soc = _clamp(
            vehicle.battery_soc - (0.6 + vehicle.speed_kmh / 120.0) * dt / 60.0, 0, 100
        )
        vehicle.coolant_temp_c = _clamp(vehicle.coolant_temp_c + random.uniform(0.0, 0.5), 65.0, 118.0)
        vehicle.engine_temp_c = _clamp(vehicle.engine_temp_c + random.uniform(-0.2, 0.6), 30.0, 95.0)
    elif vehicle.trip_state == "CHARGE":
        vehicle.speed_kmh = 0.0
        vehicle.last_move_km = 0.0
        vehicle.throttle_pct = 0.0
        vehicle.brake_pct = 0.0
        vehicle.battery_soc = _clamp(vehicle.battery_soc + random.uniform(0.3, 0.8) * dt / 60.0 * 8.0, 0, 100)
        vehicle.coolant_temp_c = _clamp(vehicle.coolant_temp_c - random.uniform(0.1, 0.5), 60.0, 95.0)
        events.append("충전 중: DC 급속 충전기 연결")
    elif vehicle.trip_state == "IDLE":
        vehicle.speed_kmh = max(0.0, vehicle.speed_kmh * 0.85)
        vehicle.last_move_km = 0.0
        vehicle.throttle_pct = 0.0
        vehicle.brake_pct = 0.0
        vehicle.coolant_temp_c = _clamp(vehicle.coolant_temp_c - random.uniform(0.0, 0.2), 55.0, 100.0)
        vehicle.engine_temp_c = _clamp(vehicle.engine_temp_c - random.uniform(0.0, 0.1), 38.0, 95.0)
    else:
        vehicle.speed_kmh = 0.0
        vehicle.last_move_km = 0.0
        vehicle.throttle_pct = 0.0
        vehicle.brake_pct = 0.0
        vehicle.ignition_on = False
        vehicle.coolant_temp_c = _clamp(vehicle.coolant_temp_c - random.uniform(0.0, 0.2), 50.0, 95.0)

    if vehicle.trip_state == "DRIVE":
        vehicle.heading_deg = (vehicle.heading_deg + random.uniform(-7.0, 7.0)) % 360
    else:
        vehicle.heading_deg = (vehicle.heading_deg + random.uniform(-1.5, 1.5)) % 360

    for tire_key in vehicle.tire_pressure_psi:
        vehicle.tire_pressure_psi[tire_key] = _clamp(
            vehicle.tire_pressure_psi[tire_key] + random.uniform(-0.12, 0.12),
            29.0,
            40.0,
        )

    vehicle.steering_deg = _clamp(vehicle.steering_deg + random.uniform(-8.0, 8.0), -90, 90)
    vehicle.cabin_temp_c = _clamp(vehicle.cabin_temp_c + random.uniform(-0.3, 0.3), 16.0, 32.0)
    vehicle.ambient_temp_c = _clamp(vehicle.ambient_temp_c + random.uniform(-0.1, 0.1), -10.0, 40.0)
    return events


def build_payload(vehicle: VehicleState, events: List[str]) -> Dict[str, Any]:
    ts = utc_now_iso()
    
    duration_s = random.randint(0, 7200)
    m, s = divmod(duration_s, 60)
    h, m = divmod(m, 60)
    duration_hms = f"{h:02d}:{m:02d}:{s:02d}"
    
    return {
        "vehicle": {
            "vehicle_id": vehicle.vehicle_id,
            "vin": vehicle.vin,
            "model": vehicle.model,
            "driver": vehicle.driver,
            "timestamp": ts
        },
        "model": vehicle.model,
        "driver": vehicle.driver,
        "location": {
            "city": vehicle.city,
            "coordinates": {
                "latitude": round(vehicle.latitude, 6),
                "longitude": round(vehicle.longitude, 6)
            },
            "heading_deg": round(vehicle.heading_deg, 2),
            "altitude_m": round(8 + (vehicle.vehicle_id[-1].__hash__() % 40), 2),
            "gps_accuracy_m": round(random.uniform(1.0, 8.0), 2)
        },
        "trip": {
            "state": vehicle.trip_state,
            "duration_s": duration_s,
            "duration_hms": duration_hms,
            "speed_kmh": round(vehicle.speed_kmh, 1),
            "odometer_km": round(vehicle.odometer_km, 2),
            "odometer_delta_km": round(vehicle.last_move_km, 4)
        },
        "battery": {
            "soc_pct": round(vehicle.battery_soc, 2),
            "health_pct": round(vehicle.battery_health, 2),
            "pack_voltage_v": round(370 + (vehicle.battery_soc - 50.0) * 0.09, 2),
            "pack_current_a": round((vehicle.speed_kmh * 0.08) * random.uniform(-1.0, 1.0) * 0.1, 2),
            "aux_12v_battery_v": round(random.uniform(12.0, 12.8), 2),
            "is_charging": vehicle.is_charging
        },
        "temperatures_c": {
            "cabin": round(vehicle.cabin_temp_c, 1),
            "ambient": round(vehicle.ambient_temp_c, 1),
            "engine": round(vehicle.engine_temp_c, 1),
            "coolant": round(vehicle.coolant_temp_c, 1)
        },
        "dynamics": {
            "traction": random.choice(["AWD", "FWD", "RWD"]),
            "acceleration_mps2": round((vehicle.throttle_pct / 100.0) * 3.2, 2),
            "brake_pct": round(vehicle.brake_pct, 1),
            "throttle_pct": round(vehicle.throttle_pct, 1),
            "steering_deg": round(vehicle.steering_deg, 1),
            "gear": "D" if vehicle.speed_kmh > 0.1 and vehicle.ignition_on else ("P" if vehicle.trip_state == "PARK" else "N"),
            "driving_mode": random.choice(["Normal", "Eco", "Sport"])
        },
        "status": {
            "is_locked": vehicle.is_locked,
            "ignition_on": vehicle.ignition_on,
            "park_brake": vehicle.trip_state in ["PARK", "IDLE"],
            "door_locked": {
                "front_left": vehicle.is_locked,
                "front_right": vehicle.is_locked,
                "rear_left": vehicle.is_locked,
                "rear_right": vehicle.is_locked
            },
            "window_position": {
                "front": random.choice(["up", "down"]),
                "rear": random.choice(["up", "down"])
            }
        },
        "diagnostics": {
            "warnings": {
                "abs_warning": random.random() < 0.03,
                "esp_warning": random.random() < 0.02,
                "tpms_warning": any(v < 31 for v in vehicle.tire_pressure_psi.values())
            },
            "tire_pressure_psi": {
                k: round(v, 2) for k, v in vehicle.tire_pressure_psi.items()
            },
            "firmware_ver": "CCU-1.6.3",
            "sensor_health": random.choice(["good", "good", "good", "attention"])
        },
        "events": events if events else ["상태 업데이트"]
    }


vehicle_states: Dict[str, VehicleState] = {v.vehicle_id: v for v in generate_vehicle_seeds()}
history_store: Dict[str, Deque[Dict[str, Any]]] = defaultdict(lambda: deque(maxlen=120))
producer_tasks: List[asyncio.Task[Any]] = []


async def stream_vehicle(vehicle_id: str) -> None:
    vehicle = vehicle_states[vehicle_id]

    async with httpx.AsyncClient() as client:
        while True:
            interval = random.uniform(INGEST_INTERVAL_MIN, INGEST_INTERVAL_MAX)
            await asyncio.sleep(interval)

            now = datetime.now(timezone.utc)
            dt = (now - vehicle.last_updated).total_seconds()
            dt = max(1.0, min(3.5, dt))
            vehicle.last_updated = now

            simulate_trip_state(vehicle, dt)
            events = update_telemetry(vehicle, dt)
            payload = build_payload(vehicle, events)

            history_store[vehicle_id].append(payload)

            vehicle.tx_buffer.append(payload)

            print(
                f"[{payload['vehicle']['timestamp']}] 🚗 {vehicle_id} | {vehicle.trip_state:<5} | "
                f"{payload['trip']['speed_kmh']:>5} km/h | SOC {payload['battery']['soc_pct']:.1f}% | 📦 버퍼: {len(vehicle.tx_buffer)}개"
            )

            # 재시도 백오프 체크
            if vehicle.last_retry_time:

                # 3회 이상 실패 시 무조건 15초, 그 전에는 지수 백오프 방식으로 접속 시도
                if vehicle.retry_count >= MAX_RETRIES:
                    backoff_delay = MAX_BACKOFF_DELAY # 15초
                else:
                    backoff_delay = RETRY_BACKOFF_BASE ** vehicle.retry_count  # 2초, 4초, 8초...

                elapsed = time.time() - vehicle.last_retry_time

                if elapsed < backoff_delay:
                    continue

            buffer_size = len(vehicle.tx_buffer)
            if buffer_size == 0:
                continue

            batch_size = min(buffer_size, 50)
            bulk_data = list(vehicle.tx_buffer)[:batch_size]

            error_msg = None # 에러 상태를 추적할 변수

            try:
                response = await client.post(
                    EXTERNAL_BATCH_URL,
                    json=bulk_data,
                    timeout=INGEST_REQUEST_TIMEOUT
                )

                if response.status_code == 200:
                    # 성공 시: 버퍼 정리 및 카운터 리셋
                    for _ in range(batch_size):
                        vehicle.tx_buffer.popleft()

                    vehicle.retry_count = 0
                    vehicle.last_retry_time = None

                else:
                    # HTTP 에러
                    error_msg = f"HTTP {response.status_code}"

            except httpx.TimeoutException:
                error_msg = "타임아웃"
            except httpx.NetworkError:
                error_msg = "네트워크 에러"
            except Exception as e:
                error_msg = f"예외 발생: {type(e).__name__}"

            if error_msg:
                vehicle.retry_count += 1
                vehicle.last_retry_time = time.time()

                if vehicle.retry_count == MAX_RETRIES:
                    print(f"💤 {vehicle_id} 최대 재시도({MAX_RETRIES}회) 도달: 지금부터는 서버 부하 방지를 위해 {int(MAX_BACKOFF_DELAY)}초 주기로 연결을 시도합니다.")
                # 어떤 에러가 나든 버퍼가 포화 상태면 오래된 데이터를 버림

                print(f"❌ 🚗 {vehicle_id} {error_msg} - 접속 연결 연속 실패 {vehicle.retry_count}회")

                if len(vehicle.tx_buffer) > 800:
                    drop_count = 100
                    for _ in range(drop_count):
                        vehicle.tx_buffer.popleft()
                    print(f"⚠ {vehicle_id} 버퍼 포화 - 오래된 데이터 {drop_count}개 삭제")

def get_envelope(vehicle_id: str) -> Optional[Dict[str, Any]]:
    history = history_store.get(vehicle_id)
    if not history:
        return None
    return history[-1]


@app.get("/", response_model=Dict[str, Any])
def root():
    first = next(iter(history_store.values()), None)
    if first is None or len(first) == 0:
        sample = generate_vehicle_seeds()[0]
        events = update_telemetry(sample, 0.8)
        payload = build_payload(sample, events)
    else:
        payload = first[-1]
    return payload


@app.get("/api/vehicles")
def list_vehicles():
    result = []
    for vehicle_id in vehicle_states:
        latest = get_envelope(vehicle_id)
        if latest:
            result.append({
                "vehicle_id": vehicle_id,
                "vin": latest.get("vehicle", {}).get("vin"),
                "model": latest.get("vehicle", {}).get("model") or latest.get("model"),
                "driver": latest.get("vehicle", {}).get("driver") or latest.get("driver"),
                "timestamp": latest.get("vehicle", {}).get("timestamp"),
                "trip_state": latest.get("trip", {}).get("state"),
                "speed_kmh": latest.get("trip", {}).get("speed_kmh"),
                "battery_soc_pct": latest.get("battery", {}).get("soc_pct"),
            })
    return {"count": len(result), "vehicles": result}


@app.get("/api/vehicles/{vehicle_id}", response_model=Dict[str, Any])
def get_vehicle(vehicle_id: str):
    payload = get_envelope(vehicle_id)
    if payload is None:
        raise HTTPException(status_code=404, detail="vehicle not found or no data yet")
    return payload


@app.get("/api/vehicles/{vehicle_id}/history")
def get_vehicle_history(vehicle_id: str, limit: int = 20):
    data = history_store.get(vehicle_id, deque())
    if not data:
        return {"vehicle_id": vehicle_id, "history": []}
    max_limit = max(1, min(limit, 120))
    return {"vehicle_id": vehicle_id, "history": list(data)[-max_limit:]}


@app.get("/api/status")
def status():
    return {
        "service": "connected-car-dummy-stream",
        "vehicles": len(vehicle_states),
        "buffered_messages_per_vehicle": {
            vehicle_id: len(msgs) for vehicle_id, msgs in history_store.items()
        },
        "now": utc_now_iso(),
    }


@app.on_event("startup")
async def startup_event() -> None:
    print("Connected Car Dummy Stream starting...")
    for vehicle_id in vehicle_states:
        task = asyncio.create_task(stream_vehicle(vehicle_id))
        producer_tasks.append(task)


@app.on_event("shutdown")
async def shutdown_event() -> None:
    for task in producer_tasks:
        task.cancel()
    await asyncio.gather(*producer_tasks, return_exceptions=True)