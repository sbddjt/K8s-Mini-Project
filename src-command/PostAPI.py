from __future__ import annotations

"""Connected Car Command API (CQRS Command side).

요청(커맨드)을 받아서 영속 저장(MongoDB)하고,
도메인 이벤트를 Kafka로 발행합니다.

이 모듈의 역할 범위(CQRS 기준)
- write model: 이벤트 원본(payload) 보존
- 이벤트 발행: Query side에서 조회 모델 갱신에 사용

주의:
- Query 모델 갱신(예: Redis set)은 여기서 하지 않습니다.
- Kafka consumer는 Query 서비스(src-query)에서 별도 구현해야 합니다.
"""

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict

from fastapi import FastAPI, HTTPException, Request
from pymongo import MongoClient

try:
    from kafka import KafkaProducer
except Exception:  # pragma: no cover - optional dependency
    KafkaProducer = None


# ------------------------------------------------------------
# 앱/로거/환경설정
# ------------------------------------------------------------
app = FastAPI(title="Connected Car Command API")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# MongoDB 연결 정보
# 운영에서는 보통 service DNS(예: mongodb-svc) 또는 Secret에서 주입.
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB = os.getenv("MONGO_DB", "car_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "car_logs")

# Kafka 연결/이벤트 정보
# KAFKA_ENABLED=false로 두면 카프카 미설치 환경에서도 API는 동작하지만 이벤트 발행만 비활성화.
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "car.telemetry.events")
KAFKA_ENABLED = os.getenv("KAFKA_ENABLED", "true").lower() != "false"
KAFKA_SEND_TIMEOUT = float(os.getenv("KAFKA_SEND_TIMEOUT_SECONDS", "3"))

# 애플리케이션 시작 시점에 바로 커넥션 핸들러를 생성하여 요청 처리 시 재사용한다.
# (매 요청마다 new MongoClient/KafkaProducer를 만들지 않아 성능/안정성 측면에서 유리)
mongo_client = MongoClient(MONGO_URI)
collection = mongo_client[MONGO_DB][MONGO_COLLECTION]

# 시작/종료 이벤트에서 실질적으로 Producer를 세팅/해제한다.
kafka_producer = None
if KAFKA_ENABLED and KafkaProducer is None:
    logger.warning("kafka-python is not installed. Kafka publishing is disabled.")


def _now_iso() -> str:
    """UTC 기준 ISO8601 문자열 생성 헬퍼.

    이벤트 발생 시각과 API 수신 시각을 추적용으로 붙인다.
    """
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _extract_vehicle_id(data: dict) -> str:
    """vehicle_id 추출 + 정합성 검증.

    현재 소스(dummy 데이터)는 vehicle_id를 사용하므로 우선 그것을 기준으로 잡고,
    하위 호환으로 car_id도 fallback으로 처리한다.
    """
    vehicle_id = data.get("vehicle_id") if isinstance(data, dict) else None
    if vehicle_id is None:
        vehicle_id = data.get("car_id")
    if not isinstance(vehicle_id, str) or not vehicle_id.strip():
        raise ValueError("vehicle_id is required as a non-empty string")
    return vehicle_id


def _build_event(vehicle_id: str, command_id: str, mongo_id: str, payload: dict) -> dict:
    """Kafka로 전달할 이벤트(Envelope) 생성.

    최소 필드는 아래 4개이며, Query/Consumer가 어떤 기준으로도 추적 가능하도록 구성한다.
    - event_id: 재시도/중복 추적용
    - event_type: 이벤트 성격 분기용
    - command_id: Command 요청 추적용
    - mongo_id: Command DB 원본 레코드 링크용
    """
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "VehicleTelemetryReceived",
        "occurred_at": _now_iso(),
        "vehicle_id": vehicle_id,
        "command_id": command_id,
        "mongo_id": mongo_id,
        "payload": payload,
    }


def _publish_event(event: dict) -> tuple[bool, str]:
    """Kafka 발행 함수.

    반환값:
    - (True, 'published'): 발행 성공
    - (False, reason): Producer 미초기화 또는 send 실패

    현재는 단일 요청에서 동기 대기로 상태를 API 응답에 반영한다.
    """
    if kafka_producer is None:
        return False, "kafka_not_configured"
    try:
        # Kafka는 비동기 send를 기본으로 하며, get(timeout=...)으로 메타데이터 수신까지 기다린다.
        # 이로 인해 응답 시간이 약간 증가할 수 있다(운영 확장 시 비동기 큐잉 방식 고려).
        future = kafka_producer.send(KAFKA_TOPIC, event)
        future.get(timeout=KAFKA_SEND_TIMEOUT)
        return True, "published"
    except Exception as exc:  # noqa: BLE001
        return False, f"publish_failed:{exc}"


@app.on_event("startup")
async def startup() -> None:
    """애플리케이션 시작 이벤트.

    1) Kafka가 활성화된 경우에만 Producer 생성
    2) value_serializer로 JSON bytes를 producer가 직접 직렬화
    """
    global kafka_producer
    if not KAFKA_ENABLED or KafkaProducer is None:
        return

    kafka_producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS.split(","),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        retries=3,
        acks="all",
    )


@app.on_event("shutdown")
async def shutdown() -> None:
    """애플리케이션 종료 이벤트.

    전송 중인 메시지를 남김없이 비우기 위해 flush 후 producer를 닫는다.
    """
    global kafka_producer
    if kafka_producer is not None:
        kafka_producer.flush(timeout=2)
        kafka_producer.close()
        kafka_producer = None


@app.get("/health")
async def health() -> dict[str, str]:
    """kube probe용 헬스체크.

    더 정확한 상태판단이 필요하면 DB/Kafka 커넥션 확인까지 추가할 수 있다.
    """
    return {"status": "ok", "service": "command-api"}


def _build_response(
    *, vehicle_id: str, command_id: str, mongo_id: str, kafka_published: bool, kafka_status: str
) -> Dict[str, Any]:
    """클라이언트 응답 포맷 표준화 헬퍼.

    프론트/테스터가 동일하게 파싱할 수 있도록 고정 키셋 제공.
    """
    return {
        "status": "success" if kafka_published else "partial_success",
        "message": f"Data saved for {vehicle_id}",
        "vehicle_id": vehicle_id,
        "command_id": command_id,
        "mongo_id": mongo_id,
        "mongo_inserted": True,
        "kafka_published": kafka_published,
        "kafka_status": kafka_status,
    }


@app.post("/api/v1/telemetry")
async def receive_telemetry(request: Request) -> Dict[str, Any]:
    """Command API 진입점.

    처리 흐름:
    1. request 바디 JSON 파싱
    2. vehicle_id 추출/검증
    3. command_id 부여 + 메타데이터(received_at) 추가
    4. MongoDB에 영속 저장
    5. Event를 Kafka에 발행
    6. 결과 반환

    CQRS에서 write DB와 이벤트 발행이 분리되는지 확인할 수 있는 핵심 경로입니다.
    """
    try:
        data = await request.json()
    except Exception as exc:
        # 잘못된 JSON은 400으로 즉시 반환
        raise HTTPException(status_code=400, detail=f"invalid_json: {exc}")

    if not isinstance(data, dict):
        # dict가 아니면 스키마 자체가 아예 맞지 않음
        raise HTTPException(status_code=400, detail="request body must be an object")

    try:
        vehicle_id = _extract_vehicle_id(data)
    except ValueError as exc:
        # 필수 식별자가 없는 경우 422
        raise HTTPException(status_code=422, detail=str(exc))

    # Mongo에 저장할 때 command_id/received_at을 추가해 추적 가능하게 만든다.
    command_id = str(uuid.uuid4())
    received_at = _now_iso()

    command_record = data.copy()
    command_record.setdefault("vehicle_id", vehicle_id)
    command_record["command_id"] = command_id
    command_record["received_at"] = received_at

    mongo_result = collection.insert_one(command_record)
    mongo_id = str(mongo_result.inserted_id)

    # 저장이 끝난 뒤 이벤트를 바로 발행한다.
    # 참고: 이 단계에서 publish 실패 시 Mongo 저장은 이미 완료된 상태이므로,
    # 데이터 불일치 방지를 위해 Outbox 패턴을 고려할 수 있다.
    event = _build_event(vehicle_id, command_id, mongo_id, command_record)
    kafka_published, kafka_status = _publish_event(event)

    response = _build_response(
        vehicle_id=vehicle_id,
        command_id=command_id,
        mongo_id=mongo_id,
        kafka_published=kafka_published,
        kafka_status=kafka_status,
    )
    response["received_at"] = received_at
    return response
