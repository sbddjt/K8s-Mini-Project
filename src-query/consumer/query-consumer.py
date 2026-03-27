import json
import os
import signal
import time
from datetime import datetime, timezone
from typing import Any, Dict

from kafka import KafkaConsumer
from kafka.errors import KafkaError
from database import redis_client
from prometheus_client import Counter, Gauge, Histogram, start_http_server

RUNNING = True
LAST_CURSOR_TS = 0
QUERY_CONSUMER_MESSAGES_TOTAL = Counter(
  "query_consumer_messages_total",
  "Total Kafka messages processed by the query consumer."
)
QUERY_CONSUMER_BATCHES_TOTAL = Counter(
  "query_consumer_batches_total",
  "Total query-consumer poll batches that produced updates."
)
REDIS_PROJECTION_UPDATES_TOTAL = Counter(
  "redis_projection_updates_total",
  "Total vehicle snapshots projected into Redis."
)
REDIS_PROJECTION_FAILURES_TOTAL = Counter(
  "redis_projection_failures_total",
  "Total Redis projection failures in the query consumer."
)
REDIS_PROJECTION_SECONDS = Histogram(
  "redis_projection_seconds",
  "Latency of projecting vehicle updates into Redis.",
  buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5)
)
QUERY_CONSUMER_UP = Gauge(
  "query_consumer_up",
  "Whether the query consumer main loop is running."
)


def _get_float(value, default=0.0):
  try:
    if value is None:
      return default
    return float(value)
  except (TypeError, ValueError):
    return default


def _coerce_str(value, default=None):
  if value is None:
    return default
  return str(value)


def _parse_epoch_ms(value: Any) -> int:
  if value is None:
    return 0
  if isinstance(value, (int, float)):
    return int(value)

  text = str(value).strip()
  if not text:
    return 0

  if text.isdigit():
    try:
      return int(text)
    except (TypeError, ValueError):
      return 0

  try:
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
      parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)
  except Exception:
    return 0


def _extract_source_event_ts(vehicle_data: dict) -> int:
  if not isinstance(vehicle_data, dict):
    return 0

  candidates = (
    vehicle_data.get("event_ts"),
    vehicle_data.get("timestamp"),
    vehicle_data.get("received_at"),
    vehicle_data.get("vehicle", {}).get("timestamp_utc") if isinstance(vehicle_data.get("vehicle"), dict) else None,
    vehicle_data.get("raw", {}).get("vehicle", {}).get("timestamp_utc") if isinstance(vehicle_data.get("raw"), dict) else None,
  )
  for candidate in candidates:
    parsed = _parse_epoch_ms(candidate)
    if parsed > 0:
      return parsed
  return 0


def _next_cursor_ts() -> int:
  global LAST_CURSOR_TS

  now_ms = int(time.time() * 1000)
  LAST_CURSOR_TS = max(now_ms, LAST_CURSOR_TS + 1)
  return LAST_CURSOR_TS


def _pick_vehicle_meta(vehicle_data: dict, key: str, default: str = "") -> str:
  raw = vehicle_data.get("raw")
  raw_vehicle = raw.get("vehicle") if isinstance(raw, dict) else None
  vehicle_root = vehicle_data.get("vehicle")
  candidates = (
    raw_vehicle.get(key) if isinstance(raw_vehicle, dict) else None,
    raw.get(key) if isinstance(raw, dict) else None,
    vehicle_root.get(key) if isinstance(vehicle_root, dict) else None,
    vehicle_data.get(key),
  )
  for value in candidates:
    if value is None:
      continue
    text = str(value).strip()
    if text:
      return text
  return default


def _build_snapshot(vehicle_data: dict) -> dict:
  vehicle_id = vehicle_data.get("vehicle_id", vehicle_data.get("vehicle", {}).get("vehicle_id"))
  if not vehicle_id:
    return {}

  location = vehicle_data.get("location", {}) or {}
  coords = location.get("coordinates") or {}
  latitude = _get_float(coords.get("latitude", location.get("latitude")), 0.0)
  longitude = _get_float(coords.get("longitude", location.get("longitude")), 0.0)
  city = location.get("city")
  state = (
    _coerce_str(vehicle_data.get("state"))
    or _coerce_str(vehicle_data.get("trip", {}).get("state"), "UNKNOWN")
    or "UNKNOWN"
  )
  received = (
    _coerce_str(vehicle_data.get("received_at"))
    or _coerce_str(vehicle_data.get("timestamp"))
    or _coerce_str(vehicle_data.get("vehicle", {}).get("timestamp_utc"))
  )
  if received is None:
    received = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

  source_event_ts = _extract_source_event_ts(vehicle_data)

  return {
    "vehicle_id": vehicle_id,
    "received_at": received,
    "event_ts": source_event_ts,
    "state": state,
    "speed_kmh": _get_float(vehicle_data.get("speed_kmh", vehicle_data.get("trip", {}).get("speed_kmh")), 0.0),
    "soc_pct": _get_float(vehicle_data.get("soc_pct", vehicle_data.get("battery", {}).get("soc_pct")), 0.0),
    "latitude": latitude,
    "longitude": longitude,
    "city": city,
    "recent_event": vehicle_data.get("recent_event"),
    "model": _pick_vehicle_meta(vehicle_data, "model", ""),
    "driver": _pick_vehicle_meta(vehicle_data, "driver", ""),
  }


def _safe_json_loads(raw: bytes | str) -> Dict[str, Any]:
    try:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        return json.loads(raw)
    except (TypeError, json.JSONDecodeError, UnicodeDecodeError):
        return {}


def _handle_sigterm(signum, frame):
  global RUNNING
  RUNNING = False


signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)


def start_consumer():
  topic = os.getenv("KAFKA_TOPIC", "car.telemetry.events")
  bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka-svc:9092")
  group_id = os.getenv("KAFKA_GROUP_ID", "vehicle-projector")
  ttl_sec = int(os.getenv("REDIS_TTL_SEC", "60"))
  poll_timeout_ms = int(os.getenv("KAFKA_POLL_TIMEOUT_MS", "1000"))
  retry_sec = int(os.getenv("KAFKA_RETRY_SEC", "3"))
  batch_size = int(os.getenv("KAFKA_BATCH_SIZE", "200"))
  update_stream = os.getenv("VEHICLE_UPDATE_STREAM", "vehicle:updates")
  stream_maxlen = int(os.getenv("VEHICLE_UPDATE_STREAM_MAXLEN", "5000"))
  active_ids_key = os.getenv("REDIS_ACTIVE_IDS_KEY", "vehicle:active_ids")
  active_ids_lex_key = os.getenv("REDIS_ACTIVE_IDS_LEX_KEY", "vehicle:active_ids:lex")
  metrics_port = int(os.getenv("METRICS_PORT", "9102"))

  consumer = None
  start_http_server(metrics_port)
  print(f"[INFO] Query consumer metrics exposed on :{metrics_port}/metrics")

  while RUNNING:
    try:
      consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap.split(","),
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="latest",
        value_deserializer=lambda m: _safe_json_loads(m),
      )
      print(f"[INFO] Kafka Consumer started. topic={topic}, bootstrap={bootstrap}, group={group_id}")
      break
    except Exception as e:
      print(f"[ERROR] KafkaConsumer init failed: {e}. retry in {retry_sec}s.")
      time.sleep(retry_sec)

  if consumer is None:
    return

  try:
    QUERY_CONSUMER_UP.set(1)
    while RUNNING:
      records = consumer.poll(timeout_ms=poll_timeout_ms, max_records=batch_size)
      updated = 0

      for _, msgs in records.items():
        for msg in msgs:
          try:
            QUERY_CONSUMER_MESSAGES_TOTAL.inc()
            vehicle_data = msg.value
            if not isinstance(vehicle_data, dict):
              continue

            vehicle_id = vehicle_data.get("vehicle_id") or vehicle_data.get("vehicle", {}).get("vehicle_id")
            if not vehicle_id:
              print(f"[WARN] missing vehicle_id: {vehicle_data}")
              continue

            key = f"vehicle:{vehicle_id}:latest"
            source_event_ts = _extract_source_event_ts(vehicle_data)
            existing_payload = _safe_json_loads(redis_client.get(key))
            existing_event_ts = _extract_source_event_ts(existing_payload) if existing_payload else 0
            if source_event_ts > 0 and existing_event_ts > source_event_ts:
              continue

            cursor_ts = _next_cursor_ts()
            latest_payload = dict(vehicle_data)
            if source_event_ts > 0:
              latest_payload["event_ts"] = source_event_ts
            latest_payload["projected_at_ms"] = cursor_ts

            with REDIS_PROJECTION_SECONDS.time():
              redis_client.set(key, json.dumps(latest_payload), ex=ttl_sec)
              redis_client.zadd(active_ids_key, {vehicle_id: cursor_ts})
              redis_client.zadd(active_ids_lex_key, {vehicle_id: 0})

            snapshot = _build_snapshot(latest_payload)
            if snapshot.get("vehicle_id"):
              payload = {
                "v": json.dumps(snapshot, ensure_ascii=False),
                "vehicle_id": vehicle_id,
                "event_ts": str(source_event_ts or cursor_ts),
                "cursor_ts": str(cursor_ts),
              }
              redis_client.xadd(update_stream, payload, maxlen=stream_maxlen, approximate=True)
            REDIS_PROJECTION_UPDATES_TOTAL.inc()
            updated += 1
          except Exception as e:
            REDIS_PROJECTION_FAILURES_TOTAL.inc()
            print(f"[ERROR] processing failed: {e}")

      if updated > 0:
        QUERY_CONSUMER_BATCHES_TOTAL.inc()
        consumer.commit()
        print(f"[INFO] Redis latest key update + offset commit: {updated} rows")

  except KafkaError as e:
    print(f"[ERROR] Kafka error: {e}")
  finally:
    QUERY_CONSUMER_UP.set(0)
    if consumer is not None:
      consumer.close()
    print("[INFO] Kafka Consumer stopped")


if __name__ == "__main__":
  start_consumer()
