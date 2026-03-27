import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from kafka import KafkaProducer
from pydantic import BaseModel
from prometheus_client import Counter, Histogram
from prometheus_fastapi_instrumentator import Instrumentator


def _get_env(name: str, default: str) -> str:
    return os.getenv(name, default).strip()


def _as_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name, str(default)).strip().lower()
    return value in {"1", "true", "yes", "on", "y"}


APP_NAME = "Connected Car Ingest API"
KAFKA_BROKERS = _get_env("KAFKA_BROKERS", "localhost:9092")
KAFKA_TOPIC = _get_env("KAFKA_TOPIC", "car.telemetry.events")
KAFKA_CLIENT_ID = _get_env("KAFKA_CLIENT_ID", "command-api-producer")
KAFKA_ENABLED = _as_bool("KAFKA_ENABLED", True)
ENABLE_SCHEMA_LOG = _as_bool("ENABLE_SCHEMA_LOG", False)

app = FastAPI(title=APP_NAME)

INGEST_REQUESTS_TOTAL = Counter(
    "ingest_requests_total",
    "Total telemetry ingest requests accepted by the command API.",
)
KAFKA_PUBLISH_SUCCESS_TOTAL = Counter(
    "kafka_publish_success_total",
    "Total telemetry events successfully published to Kafka.",
)
KAFKA_PUBLISH_FAILURE_TOTAL = Counter(
    "kafka_publish_failure_total",
    "Total telemetry events that failed to publish to Kafka.",
)
KAFKA_PUBLISH_LATENCY_SECONDS = Histogram(
    "kafka_publish_latency_seconds",
    "Latency of Kafka publish operations from the command API.",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)


producer: Optional[KafkaProducer] = None

Instrumentator(excluded_handlers=["/metrics"], should_group_status_codes=False).instrument(app).expose(
    app,
    include_in_schema=False,
)


def _init_producer() -> Optional[KafkaProducer]:
    if not KAFKA_ENABLED:
        return None

    try:
        initialized = KafkaProducer(
            bootstrap_servers=[s.strip() for s in KAFKA_BROKERS.split(",") if s.strip()],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else None,
            acks="all",
            retries=5,
            client_id=KAFKA_CLIENT_ID,
        )
        print(f"[producer] initialized: brokers={KAFKA_BROKERS}, topic={KAFKA_TOPIC}")
        return initialized
    except Exception as exc:
        print(f"[producer] init failed: {exc}")
        return None


def _get_producer() -> Optional[KafkaProducer]:
    global producer

    if producer is not None:
        return producer

    producer = _init_producer()
    return producer


producer = _init_producer()


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
    vehicle: VehicleInfo
    location: LocationInfo
    trip: TripInfo
    battery: BatteryInfo
    temperatures_c: Dict[str, float]
    dynamics: Dict[str, Any]
    status: Dict[str, Any]
    diagnostics: Dict[str, Any]
    events: List[str]


def _build_kafka_message(payload: Dict[str, Any]) -> Dict[str, Any]:
    vehicle = payload["vehicle"]
    location = payload["location"]["coordinates"]
    trip = payload["trip"]
    battery = payload["battery"]
    events = payload.get("events", [])

    return {
        "vehicle_id": vehicle["vehicle_id"],
        "timestamp": vehicle["timestamp_utc"],
        "received_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "state": trip.get("state"),
        "speed_kmh": trip.get("speed_kmh"),
        "soc_pct": battery.get("soc_pct"),
        "location": {
            "latitude": location.get("latitude"),
            "longitude": location.get("longitude"),
        },
        "recent_event": events[-1] if events else None,
        "raw": payload,
    }


@app.post("/api/query/telemetry")
async def ingest_telemetry(data: ConnectedCarData):
    global producer

    INGEST_REQUESTS_TOTAL.inc()
    active_producer = _get_producer()
    if not KAFKA_ENABLED or active_producer is None:
        KAFKA_PUBLISH_FAILURE_TOTAL.inc()
        raise HTTPException(
            status_code=503,
            detail="Kafka producer is not available. Check command service configuration."
        )

    payload = data.model_dump()
    if ENABLE_SCHEMA_LOG:
        print("[ingest] payload:", json.dumps(payload, ensure_ascii=False)[:500])

    kafka_payload = _build_kafka_message(payload)

    try:
        with KAFKA_PUBLISH_LATENCY_SECONDS.time():
            future = active_producer.send(
                KAFKA_TOPIC,
                key=payload["vehicle"]["vehicle_id"],
                value=kafka_payload,
            )
            # 비동기적으로 에러 로깅
            future.add_errback(lambda exc: print(f"[producer] send error: {exc}"))
            future.get(timeout=2.0)
        KAFKA_PUBLISH_SUCCESS_TOTAL.inc()
        return {"status": "success", "processed_vehicle": payload["vehicle"]["vehicle_id"]}
    except Exception as exc:
        KAFKA_PUBLISH_FAILURE_TOTAL.inc()
        producer = None
        print(f"[ingest] send failed: {exc}")
        raise HTTPException(status_code=500, detail="Failed to publish telemetry event.")


@app.get("/health")
async def health_check():
    return {
        "status": "ok",
        "service": APP_NAME,
        "kafka_enabled": KAFKA_ENABLED,
        "kafka_topic": KAFKA_TOPIC,
        "kafka_brokers": KAFKA_BROKERS,
    }
