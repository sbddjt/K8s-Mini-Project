#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import asyncio
import json
import os
import time
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Dict, Iterable, List, Optional, Tuple

from fastapi import FastAPI, HTTPException, Request, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse
from pymongo import MongoClient
from pymongo.collection import Collection
from prometheus_client import Counter, Gauge
from prometheus_fastapi_instrumentator import Instrumentator

from database import redis_client

app = FastAPI(title="Connected Car Query API")

QUERY_LIST_REQUESTS_TOTAL = Counter(
    "query_vehicle_list_requests_total",
    "Total vehicle list-style query requests handled by the query API.",
    labelnames=("endpoint",),
)
QUERY_DETAIL_REQUESTS_TOTAL = Counter(
    "query_vehicle_detail_requests_total",
    "Total vehicle detail and history requests handled by the query API.",
    labelnames=("endpoint",),
)
QUERY_REDIS_FAILURES_TOTAL = Counter(
    "query_redis_failures_total",
    "Total Redis read failures observed by the query API.",
    labelnames=("operation",),
)
QUERY_MONGO_FAILURES_TOTAL = Counter(
    "query_mongo_failures_total",
    "Total MongoDB read failures observed by the query API.",
    labelnames=("operation",),
)
QUERY_WEBSOCKET_CONNECTIONS = Gauge(
    "query_websocket_connections_active",
    "Number of active vehicle websocket subscriptions.",
)

Instrumentator(excluded_handlers=["/metrics"], should_group_status_codes=False).instrument(app).expose(
    app,
    include_in_schema=False,
)

REDIS_KEY_PATTERN = os.getenv("REDIS_KEY_PATTERN", "vehicle:*:latest")
REDIS_ACTIVE_IDS_KEY = os.getenv("REDIS_ACTIVE_IDS_KEY", "vehicle:active_ids")
REDIS_ACTIVE_IDS_LEX_KEY = os.getenv("REDIS_ACTIVE_IDS_LEX_KEY", "vehicle:active_ids:lex")
REDIS_ACTIVE_WINDOW_MS = int(os.getenv("REDIS_ACTIVE_WINDOW_MS", "120000"))
VEHICLE_UPDATE_STREAM = os.getenv("VEHICLE_UPDATE_STREAM", "vehicle:updates")
VEHICLE_UPDATE_BATCH = int(os.getenv("VEHICLE_UPDATE_BATCH", "500"))
DEFAULT_LIMIT = int(os.getenv("VEHICLE_LIST_DEFAULT_LIMIT", "500"))
MAX_LIMIT = int(os.getenv("VEHICLE_LIST_MAX_LIMIT", "5000"))
MONGO_URI = os.getenv("MONGO_URI", "").strip()
MONGO_DB = os.getenv("MONGO_DB", "car_db").strip()
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "telemetry_history").strip()

_mongo_collection: Optional[Collection] = None


def _get_mongo_collection() -> Optional[Collection]:
    global _mongo_collection

    if _mongo_collection is not None:
        return _mongo_collection

    if not MONGO_URI:
        return None

    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=1500)
        client.admin.command("ping")
        _mongo_collection = client[MONGO_DB][MONGO_COLLECTION]
    except Exception:
        _mongo_collection = None

    return _mongo_collection


def _safe_json_loads(raw: Any) -> Optional[Dict[str, Any]]:
    if raw is None:
        return None

    if isinstance(raw, (bytes, bytearray, memoryview)):
        raw = bytes(raw).decode("utf-8", errors="replace")
    elif not isinstance(raw, str):
        raw = str(raw)

    try:
        value = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return None

    return value if isinstance(value, dict) else None


def _safe_json_dumps(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, default=str)
    except (TypeError, ValueError):
        return "{}"


def _to_number(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_str(value: Any, default: str = "") -> str:
  if value is None:
    return default
  return str(value)


def _coalesce_str(*values: Any) -> str:
  for value in values:
    if value is None:
      continue
    value = str(value).strip()
    if value:
      return value
  return ""


def _extract_vehicle_meta(payload: Dict[str, Any], key: str, default: str = "") -> str:
  raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
  raw_vehicle = raw.get("vehicle") if isinstance(raw.get("vehicle"), dict) else {}
  vehicle_root = payload.get("vehicle") if isinstance(payload.get("vehicle"), dict) else {}
  return _coalesce_str(
    raw_vehicle.get(key),
    payload.get(key),
    vehicle_root.get(key),
    raw.get(key),
    default,
  )


def _parse_epoch_ms(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value)

    value_text = str(value).strip()
    if not value_text:
        return 0

    if value_text.isdigit():
        try:
            return int(value_text)
        except (TypeError, ValueError):
            pass

    try:
        dt = datetime.fromisoformat(value_text.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    except Exception:
        return 0


def _extract_vehicle_id(key: str) -> Optional[str]:
    # key format expected: vehicle:{vehicle_id}:latest
    parts = key.split(":")
    if len(parts) < 3:
        return None
    return parts[1]


def _extract_stream_cursor(fields: Dict[str, Any]) -> int:
    if not isinstance(fields, dict):
        return 0

    cursor_ts = _to_int(fields.get("cursor_ts"))
    if cursor_ts > 0:
        return cursor_ts
    return _to_int(fields.get("event_ts"))


def _prune_inactive_vehicle_ids() -> int:
    cutoff_ms = max(0, int(time.time() * 1000) - REDIS_ACTIVE_WINDOW_MS)

    try:
        stale_ids = redis_client.zrangebyscore(REDIS_ACTIVE_IDS_KEY, 0, cutoff_ms)
        if stale_ids:
            redis_client.zrem(REDIS_ACTIVE_IDS_KEY, *stale_ids)
            try:
                redis_client.zrem(REDIS_ACTIVE_IDS_LEX_KEY, *stale_ids)
            except Exception:
                pass
        return _to_int(redis_client.zcard(REDIS_ACTIVE_IDS_KEY))
    except Exception:
        return 0


def _iter_active_vehicle_ids() -> Iterable[str]:
    seen: set[str] = set()

    _prune_inactive_vehicle_ids()
    try:
        members = redis_client.zrange(REDIS_ACTIVE_IDS_KEY, 0, -1)
    except Exception:
        members = []

    for member in members:
        vehicle_id = _coerce_str(member).strip()
        if not vehicle_id or vehicle_id in seen:
            continue
        seen.add(vehicle_id)
        yield vehicle_id

    if seen:
        return

    for key in _iter_latest_keys():
        if not isinstance(key, str):
            key = str(key)
        vehicle_id = _extract_vehicle_id(key)
        if not vehicle_id or vehicle_id in seen:
            continue
        seen.add(vehicle_id)
        yield vehicle_id


def _extract_latest_event_ts(vehicle: Dict[str, Any]) -> int:
    if not isinstance(vehicle, dict):
        return 0
    event_ts = _to_int(vehicle.get("event_ts"))
    if event_ts > 0:
        return event_ts
    return _parse_epoch_ms(vehicle.get("received_at"))


def _build_full_vehicle(payload: Dict[str, Any]) -> Dict[str, Any]:
    location = payload.get("location") if isinstance(payload.get("location"), dict) else {}
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    raw_vehicle = raw.get("vehicle") if isinstance(raw.get("vehicle"), dict) else {}
    vehicle_root = payload.get("vehicle") if isinstance(payload.get("vehicle"), dict) else {}
    raw_trip = payload.get("trip") if isinstance(payload.get("trip"), dict) else {}
    raw_battery = payload.get("battery") if isinstance(payload.get("battery"), dict) else {}
    raw_location = raw.get("location") if isinstance(raw.get("location"), dict) else {}

    vehicle_id = _coerce_str(
        payload.get("vehicle_id")
        or raw_vehicle.get("vehicle_id")
        or vehicle_root.get("vehicle_id")
    )

    received_at = _coerce_str(
        payload.get("received_at")
        or payload.get("timestamp")
        or raw_vehicle.get("timestamp_utc")
        or vehicle_root.get("timestamp_utc")
        or payload.get("event_ts")
        or str(int(time.time() * 1000))
    )

    latitude = _to_number(location.get("latitude", payload.get("latitude")), 0.0)
    longitude = _to_number(location.get("longitude", payload.get("longitude")), 0.0)

    if latitude == 0.0 and longitude == 0.0:
        coords = raw_location.get("coordinates") if isinstance(raw_location.get("coordinates"), dict) else None
        if coords:
            latitude = _to_number(coords.get("latitude"), 0.0)
            longitude = _to_number(coords.get("longitude"), 0.0)

    city = _coerce_str(location.get("city"))
    if not city and raw_location.get("city"):
        city = _coerce_str(raw_location.get("city"))

    return {
        "vehicle_id": vehicle_id,
        "timestamp": _coerce_str(payload.get("timestamp") or raw_vehicle.get("timestamp_utc")),
        "received_at": received_at,
        "event_ts": _extract_latest_event_ts({"event_ts": payload.get("event_ts"), "received_at": received_at}),
        "state": _coerce_str(payload.get("state") or raw_trip.get("state") or "UNKNOWN"),
        "speed_kmh": _to_number(payload.get("speed_kmh", raw_trip.get("speed_kmh"))),
        "soc_pct": _to_number(payload.get("soc_pct", raw_battery.get("soc_pct"))),
        "model": _extract_vehicle_meta(payload, "model", ""),
        "driver": _extract_vehicle_meta(payload, "driver", ""),
        "location": {
            "latitude": latitude,
            "longitude": longitude,
            "city": city,
            "heading_deg": _to_number(location.get("heading_deg")),
            "altitude_m": _to_number(location.get("altitude_m")),
            "gps_accuracy_m": _to_number(location.get("gps_accuracy_m")),
        },
        "recent_event": payload.get("recent_event"),
        "raw": payload,
    }


def _build_vehicle_snapshot(payload: Dict[str, Any]) -> Dict[str, Any]:
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    raw_vehicle = raw.get("vehicle") if isinstance(raw.get("vehicle"), dict) else {}

    city = payload.get("city")
    if city is None:
        city = raw.get("location", {}).get("city") if isinstance(raw, dict) else None

    location = payload.get("location") if isinstance(payload.get("location"), dict) else {}
    latitude = _to_number(location.get("latitude", payload.get("latitude")), 0.0)
    longitude = _to_number(location.get("longitude", payload.get("longitude")), 0.0)

    if latitude == 0.0 and longitude == 0.0:
        coords = raw.get("location", {}).get("coordinates") if isinstance(raw.get("location"), dict) else None
        if isinstance(coords, dict):
            latitude = _to_number(coords.get("latitude"), 0.0)
            longitude = _to_number(coords.get("longitude"), 0.0)

    received_at = _coerce_str(
        payload.get("received_at")
        or payload.get("timestamp")
        or raw_vehicle.get("timestamp_utc")
        or payload.get("vehicle", {}).get("timestamp_utc")
        or payload.get("event_ts")
        or str(int(time.time() * 1000))
    )

    return {
        "vehicle_id": _coerce_str(
            payload.get("vehicle_id")
            or raw_vehicle.get("vehicle_id")
            or payload.get("vehicle", {}).get("vehicle_id")
        ),
        "received_at": received_at,
        "event_ts": _extract_latest_event_ts(payload),
        "state": _coerce_str(payload.get("state", payload.get("trip", {}).get("state", "UNKNOWN"))),
        "speed_kmh": _to_number(payload.get("speed_kmh", payload.get("trip", {}).get("speed_kmh"))),
        "soc_pct": _to_number(payload.get("soc_pct", payload.get("battery", {}).get("soc_pct"))),
        "latitude": latitude,
        "longitude": longitude,
        "city": _coerce_str(city),
        "recent_event": payload.get("recent_event"),
        "model": _extract_vehicle_meta(payload, "model", ""),
        "driver": _extract_vehicle_meta(payload, "driver", ""),
    }


def _build_vehicle_summary(payload: Dict[str, Any]) -> Dict[str, Any]:
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    location = payload.get("location") if isinstance(payload.get("location"), dict) else {}
    raw_vehicle = raw.get("vehicle") if isinstance(raw.get("vehicle"), dict) else {}

    city = _coerce_str(location.get("city"), "")
    if not city and isinstance(raw, dict):
        city = _coerce_str(raw.get("location", {}).get("city"), "")

    latitude = _to_number(location.get("latitude", payload.get("latitude")), 0.0)
    longitude = _to_number(location.get("longitude", payload.get("longitude")), 0.0)
    if latitude == 0.0 and longitude == 0.0:
        coords = location.get("coordinates")
        if isinstance(coords, dict):
            latitude = _to_number(coords.get("latitude"), 0.0)
            longitude = _to_number(coords.get("longitude"), 0.0)

    return {
        "vehicle_id": _coerce_str(
            payload.get("vehicle_id")
            or raw_vehicle.get("vehicle_id")
            or payload.get("vehicle", {}).get("vehicle_id")
        ),
        "received_at": _coerce_str(
            payload.get("received_at")
            or payload.get("timestamp")
            or payload.get("event_ts")
            or raw.get("vehicle", {}).get("timestamp_utc")
            or str(int(time.time() * 1000))
        ),
        "event_ts": _extract_latest_event_ts(payload),
        "state": _coerce_str(payload.get("state", payload.get("trip", {}).get("state", "UNKNOWN"))),
        "speed_kmh": _to_number(payload.get("speed_kmh", payload.get("trip", {}).get("speed_kmh"))),
        "soc_pct": _to_number(payload.get("soc_pct", payload.get("battery", {}).get("soc_pct"))),
        "latitude": latitude,
        "longitude": longitude,
        "city": city,
        "recent_event": payload.get("recent_event"),
        "model": _extract_vehicle_meta(payload, "model", ""),
        "driver": _extract_vehicle_meta(payload, "driver", ""),
    }


@app.get("/api/vehicles/replay")
def get_vehicle_replay(
    seconds: int = Query(default=180, ge=10, le=1800),
    bucket_ms: int = Query(default=1000, alias="bucketMs", ge=250, le=10000),
    limit: int = Query(default=180, ge=10, le=600),
) -> Dict[str, Any]:
    QUERY_LIST_REQUESTS_TOTAL.labels(endpoint="replay").inc()
    collection = _get_mongo_collection()
    if collection is None:
        return {
            "source": "mongo",
            "count": 0,
            "frames": [],
            "detail": "MongoDB replay source is not configured",
        }

    now_ms = int(time.time() * 1000)
    since_ms = now_ms - (seconds * 1000)
    max_docs = min(max(limit * 250, 500), 50000)

    try:
        cursor = collection.find(
            {"created_at": {"$gte": datetime.fromtimestamp(since_ms / 1000, timezone.utc)}},
            projection={"_id": 0},
        ).sort("created_at", 1).limit(max_docs)
        documents = list(cursor)
    except Exception as exc:
        QUERY_MONGO_FAILURES_TOTAL.labels(operation="replay").inc()
        raise HTTPException(status_code=503, detail=f"failed to load replay data from MongoDB: {exc}")

    if not documents:
        return {
            "source": "mongo",
            "count": 0,
            "frames": [],
            "detail": "No replay data found in MongoDB",
        }

    frames: List[Dict[str, Any]] = []
    current_bucket: Optional[int] = None
    current_state: Dict[str, Dict[str, Any]] = {}

    def flush_frame(bucket: Optional[int]) -> None:
        if bucket is None or not current_state:
            return

        frames.append(
            {
                "frame_ts": bucket,
                "vehicles": sorted(current_state.values(), key=lambda item: _coerce_str(item.get("vehicle_id"))),
            }
        )

    for document in documents:
        snapshot = _build_vehicle_snapshot(document if isinstance(document, dict) else {})
        vehicle_id = _coerce_str(snapshot.get("vehicle_id"))
        if not vehicle_id:
            continue

        event_ms = _extract_latest_event_ts(document if isinstance(document, dict) else {})
        if event_ms <= 0:
            event_ms = _parse_epoch_ms(document.get("created_at") if isinstance(document, dict) else None)
        if event_ms <= 0:
            continue

        bucket = max(since_ms, (event_ms // bucket_ms) * bucket_ms)

        if current_bucket is None:
            current_bucket = bucket
        elif bucket != current_bucket:
            flush_frame(current_bucket)
            current_bucket = bucket

        snapshot["event_ts"] = event_ms
        current_state[vehicle_id] = snapshot

    flush_frame(current_bucket)

    if len(frames) > limit:
        frames = frames[-limit:]

    return {
        "source": "mongo",
        "count": len(frames),
        "frames": frames,
        "since_ms": since_ms,
        "bucket_ms": bucket_ms,
    }


def _vehicle_matches_filter(
    vehicle: Dict[str, Any],
    *,
    vehicle_id: Optional[str],
    include_vehicle_id: Optional[str],
    state: Optional[str],
    city: Optional[str],
    min_speed: Optional[float],
    max_speed: Optional[float],
    since_ms: Optional[int],
    min_lat: Optional[float],
    max_lat: Optional[float],
    min_lng: Optional[float],
    max_lng: Optional[float],
) -> bool:
    location = vehicle.get("location")
    if not isinstance(location, dict):
        location = {}

    always_include = bool(include_vehicle_id and vehicle.get("vehicle_id") == include_vehicle_id)

    if vehicle_id and vehicle.get("vehicle_id") != vehicle_id:
        return False

    if state and vehicle.get("state") != state:
        return False

    vehicle_city = vehicle.get("city")
    if vehicle_city is None:
        vehicle_city = location.get("city")

    if city and str(vehicle_city or "").lower() != city.lower():
        return False

    if since_ms:
        latest_ts = _extract_latest_event_ts(vehicle)
        if latest_ts <= since_ms:
            return False

    if min_speed is not None and _to_number(vehicle.get("speed_kmh")) < min_speed:
        return False

    if max_speed is not None and _to_number(vehicle.get("speed_kmh")) > max_speed:
        return False

    latitude = _to_number(vehicle.get("latitude", location.get("latitude")), 0.0)
    longitude = _to_number(vehicle.get("longitude", location.get("longitude")), 0.0)

    if always_include:
        return True

    if min_lat is not None and latitude < min_lat:
        return False
    if max_lat is not None and latitude > max_lat:
        return False
    if min_lng is not None and longitude < min_lng:
        return False
    if max_lng is not None and longitude > max_lng:
        return False

    return True


def _iter_latest_keys() -> Iterable[str]:
    return redis_client.scan_iter(match=REDIS_KEY_PATTERN)


def _latest_payloads() -> Iterable[Dict[str, Any]]:
    vehicle_ids = list(_iter_active_vehicle_ids())
    if not vehicle_ids:
        return

    keys = [f"vehicle:{vehicle_id}:latest" for vehicle_id in vehicle_ids]

    try:
        raw_values = redis_client.mget(keys)
    except Exception:
        raw_values = [redis_client.get(key) for key in keys]

    for vehicle_id, raw_value in zip(vehicle_ids, raw_values):
        if not raw_value:
            continue

        payload = _safe_json_loads(raw_value)
        if not isinstance(payload, dict):
            continue

        payload.setdefault("vehicle_id", vehicle_id)
        yield payload


def _search_vehicle_ids(query_text: str, limit: int) -> Tuple[List[str], int]:
    normalized_limit = min(max(0, limit), 100)
    total_count = _prune_inactive_vehicle_ids()
    query_text = query_text.strip()

    if not query_text or normalized_limit == 0:
        return [], total_count

    matches: List[str] = []
    seen: set[str] = set()
    upper_query = query_text.upper()

    try:
        lex_matches = redis_client.zrangebylex(
            REDIS_ACTIVE_IDS_LEX_KEY,
            f"[{query_text}",
            f"[{query_text}\xff",
            start=0,
            num=max(normalized_limit * 3, normalized_limit),
        )
    except Exception:
        lex_matches = []

    for vehicle_id in lex_matches:
        normalized_vehicle_id = _coerce_str(vehicle_id).strip()
        if not normalized_vehicle_id or normalized_vehicle_id in seen:
            continue
        if redis_client.zscore(REDIS_ACTIVE_IDS_KEY, normalized_vehicle_id) is None:
            continue
        matches.append(normalized_vehicle_id)
        seen.add(normalized_vehicle_id)
        if len(matches) >= normalized_limit:
            return matches, total_count

    if matches:
        return matches, total_count

    for vehicle_id in _iter_active_vehicle_ids():
        normalized_vehicle_id = _coerce_str(vehicle_id).strip()
        if not normalized_vehicle_id or normalized_vehicle_id in seen:
            continue
        if upper_query not in normalized_vehicle_id.upper():
            continue
        matches.append(normalized_vehicle_id)
        seen.add(normalized_vehicle_id)
        if len(matches) >= normalized_limit:
            break

    return matches, total_count


def _collect_vehicles_from_latest(
    *,
    vehicle_id: Optional[str] = None,
    include_vehicle_id: Optional[str] = None,
    state: Optional[str] = None,
    city: Optional[str] = None,
    min_speed: Optional[float] = None,
    max_speed: Optional[float] = None,
    since_ms: Optional[int] = None,
    min_lat: Optional[float] = None,
    max_lat: Optional[float] = None,
    min_lng: Optional[float] = None,
    max_lng: Optional[float] = None,
    compact: bool = False,
    summary: bool = False,
) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for payload in _latest_payloads():
        if summary:
            candidate = _build_vehicle_summary(payload)
        elif compact:
            candidate = _build_vehicle_snapshot(payload)
        else:
            candidate = _build_full_vehicle(payload)
        if _vehicle_matches_filter(
            candidate,
            vehicle_id=vehicle_id,
            include_vehicle_id=include_vehicle_id,
            state=state,
            city=city,
            min_speed=min_speed,
            max_speed=max_speed,
            since_ms=since_ms,
            min_lat=min_lat,
            max_lat=max_lat,
            min_lng=min_lng,
            max_lng=max_lng,
        ):
            items.append(candidate)

    items.sort(key=lambda item: str(item.get("vehicle_id")))
    return items


def _collect_delta_since(
    since_ms: int,
    *,
    compact: bool = True,
    summary: bool = False,
) -> Tuple[List[Dict[str, Any]], int]:
    if since_ms < 0:
        since_ms = 0

    updates: Dict[str, Tuple[int, Dict[str, Any]]] = {}
    latest_cursor = since_ms
    page_max = "+"

    while True:
        try:
            entries = redis_client.xrevrange(
                VEHICLE_UPDATE_STREAM,
                max=page_max,
                min="-",
                count=VEHICLE_UPDATE_BATCH,
            )
        except Exception:
            return [], latest_cursor

        if not entries:
            break

        reached_cutoff = False
        last_entry_id: Optional[str] = None

        for entry_id, fields in entries:
            last_entry_id = entry_id if isinstance(entry_id, str) else str(entry_id)
            if not isinstance(fields, dict):
                continue

            cursor_ts = _extract_stream_cursor(fields)
            if cursor_ts <= since_ms:
                reached_cutoff = True
                break

            latest_cursor = max(latest_cursor, cursor_ts)
            raw_snapshot = fields.get("v")
            payload = _safe_json_loads(raw_snapshot)
            if payload is None and isinstance(raw_snapshot, dict):
                payload = dict(raw_snapshot)

            if not isinstance(payload, dict):
                continue

            payload = dict(payload)
            payload["event_ts"] = _to_int(fields.get("event_ts")) or payload.get("event_ts")
            payload["cursor_ts"] = cursor_ts
            vehicle_id = payload.get("vehicle_id")
            if not vehicle_id:
                continue

            if summary:
                built = _build_vehicle_summary(payload)
            elif compact:
                built = _build_vehicle_snapshot(payload)
            else:
                built = _build_full_vehicle(payload)

            built["cursor_ts"] = cursor_ts
            existing = updates.get(vehicle_id)
            if existing is None or existing[0] < cursor_ts:
                updates[vehicle_id] = (cursor_ts, built)

        if reached_cutoff or len(entries) < VEHICLE_UPDATE_BATCH or not last_entry_id:
            break

        page_max = f"({last_entry_id}"

    return [updates[key][1] for key in sorted(updates.keys())], latest_cursor


def _filter_updates_by(
    updates: List[Dict[str, Any]],
    *,
    vehicle_id: Optional[str],
    include_vehicle_id: Optional[str],
    state: Optional[str],
    city: Optional[str],
    min_speed: Optional[float],
    max_speed: Optional[float],
    min_lat: Optional[float],
    max_lat: Optional[float],
    min_lng: Optional[float],
    max_lng: Optional[float],
) -> List[Dict[str, Any]]:
    if not updates:
        return []

    if not (
        vehicle_id or
        include_vehicle_id or
        state or
        city or
        min_speed is not None or
        max_speed is not None or
        min_lat is not None or
        max_lat is not None or
        min_lng is not None or
        max_lng is not None
    ):
        return updates

    filtered: List[Dict[str, Any]] = []
    for item in updates:
        location = item.get("location")
        if not isinstance(location, dict):
            location = {}

        always_include = bool(include_vehicle_id and item.get("vehicle_id") == include_vehicle_id)

        if vehicle_id and item.get("vehicle_id") != vehicle_id:
            continue
        if state and item.get("state") != state:
            continue
        item_city = item.get("city")
        if item_city is None:
            item_city = location.get("city")
        if city and str(item_city or "").lower() != city.lower():
            continue
        if min_speed is not None and _to_number(item.get("speed_kmh")) < min_speed:
            continue
        if max_speed is not None and _to_number(item.get("speed_kmh")) > max_speed:
            continue
        latitude = _to_number(item.get("latitude", location.get("latitude")), 0.0)
        longitude = _to_number(item.get("longitude", location.get("longitude")), 0.0)
        if always_include:
            filtered.append(item)
            continue
        if min_lat is not None and latitude < min_lat:
            continue
        if max_lat is not None and latitude > max_lat:
            continue
        if min_lng is not None and longitude < min_lng:
            continue
        if max_lng is not None and longitude > max_lng:
            continue
        filtered.append(item)

    return filtered


def _latest_stream_id() -> int:
    try:
        entries = redis_client.xrevrange(VEHICLE_UPDATE_STREAM, count=1)
    except Exception:
        return 0

    for _, fields in entries:
        if not isinstance(fields, dict):
            continue
        cursor_ts = _extract_stream_cursor(fields)
        if cursor_ts > 0:
            return cursor_ts

    return 0


def _sse_event(payload: Dict[str, Any]) -> str:
    return f"data: {_safe_json_dumps(payload)}\n\n"


@app.get("/health")
def health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "service": "Connected Car Query API",
        "redis_key_pattern": REDIS_KEY_PATTERN,
        "stream": VEHICLE_UPDATE_STREAM,
    }


@app.get("/api/vehicles/ids")
def list_vehicle_ids() -> Dict[str, Any]:
    QUERY_LIST_REQUESTS_TOTAL.labels(endpoint="ids").inc()
    ids = sorted(
        {
            _coerce_str(payload.get("vehicle_id"))
            for payload in _latest_payloads()
            if _coerce_str(payload.get("vehicle_id"))
        },
        key=lambda value: value,
    )

    return {
        "count": len(ids),
        "vehicle_ids": ids,
        "stream_last_id": _latest_stream_id(),
    }


@app.get("/api/vehicles/search")
def search_vehicle_ids(
    q: str = Query(default="", alias="q"),
    limit: int = Query(default=20, ge=0, le=100),
) -> Dict[str, Any]:
    QUERY_LIST_REQUESTS_TOTAL.labels(endpoint="search").inc()
    matches, total_count = _search_vehicle_ids(q, limit)

    return {
        "query": q,
        "count": len(matches),
        "total_count": total_count,
        "vehicle_ids": matches,
        "stream_last_id": _latest_stream_id(),
    }


@app.get("/api/vehicles")
def list_vehicles(
    state: Optional[str] = Query(default=None),
    city: Optional[str] = Query(default=None),
    since: int = Query(default=0, alias="since"),
    vehicle_id: Optional[str] = Query(default=None),
    include_vehicle_id: Optional[str] = Query(default=None, alias="includeVehicleId"),
    min_speed: Optional[float] = Query(default=None, alias="minSpeed"),
    max_speed: Optional[float] = Query(default=None, alias="maxSpeed"),
    min_lat: Optional[float] = Query(default=None, alias="minLat"),
    max_lat: Optional[float] = Query(default=None, alias="maxLat"),
    min_lng: Optional[float] = Query(default=None, alias="minLng"),
    max_lng: Optional[float] = Query(default=None, alias="maxLng"),
    limit: int = Query(default=DEFAULT_LIMIT),
) -> Dict[str, Any]:
    QUERY_LIST_REQUESTS_TOTAL.labels(endpoint="vehicles").inc()
    normalized_limit = min(max(1, limit), MAX_LIMIT)
    vehicles = _collect_vehicles_from_latest(
        vehicle_id=vehicle_id,
        include_vehicle_id=include_vehicle_id,
        state=state,
        city=city,
        min_speed=min_speed,
        max_speed=max_speed,
        since_ms=since or None,
        min_lat=min_lat,
        max_lat=max_lat,
        min_lng=min_lng,
        max_lng=max_lng,
        compact=False,
    )

    return {
        "count": min(len(vehicles), normalized_limit),
        "vehicles": vehicles[:normalized_limit],
        "stream_last_id": _latest_stream_id(),
    }


@app.get("/api/vehicles/list")
def list_vehicles_light(
    state: Optional[str] = Query(default=None),
    city: Optional[str] = Query(default=None),
    since: int = Query(default=0, alias="since"),
    vehicle_id: Optional[str] = Query(default=None),
    include_vehicle_id: Optional[str] = Query(default=None, alias="includeVehicleId"),
    min_speed: Optional[float] = Query(default=None, alias="minSpeed"),
    max_speed: Optional[float] = Query(default=None, alias="maxSpeed"),
    min_lat: Optional[float] = Query(default=None, alias="minLat"),
    max_lat: Optional[float] = Query(default=None, alias="maxLat"),
    min_lng: Optional[float] = Query(default=None, alias="minLng"),
    max_lng: Optional[float] = Query(default=None, alias="maxLng"),
    limit: int = Query(default=DEFAULT_LIMIT),
) -> Dict[str, Any]:
    QUERY_LIST_REQUESTS_TOTAL.labels(endpoint="list").inc()
    normalized_limit = min(max(1, limit), MAX_LIMIT)
    vehicles = _collect_vehicles_from_latest(
        vehicle_id=vehicle_id,
        include_vehicle_id=include_vehicle_id,
        state=state,
        city=city,
        min_speed=min_speed,
        max_speed=max_speed,
        since_ms=since or None,
        min_lat=min_lat,
        max_lat=max_lat,
        min_lng=min_lng,
        max_lng=max_lng,
        compact=True,
        summary=True,
    )

    return {
        "type": "snapshot",
        "count": min(len(vehicles), normalized_limit),
        "vehicles": vehicles[:normalized_limit],
        "stream_last_id": _latest_stream_id(),
    }


@app.get("/api/vehicles/query")
def query_vehicles(
    state: Optional[str] = Query(default=None),
    city: Optional[str] = Query(default=None),
    since: int = Query(default=0, alias="since"),
    vehicle_id: Optional[str] = Query(default=None),
    include_vehicle_id: Optional[str] = Query(default=None, alias="includeVehicleId"),
    min_speed: Optional[float] = Query(default=None, alias="minSpeed"),
    max_speed: Optional[float] = Query(default=None, alias="maxSpeed"),
    min_lat: Optional[float] = Query(default=None, alias="minLat"),
    max_lat: Optional[float] = Query(default=None, alias="maxLat"),
    min_lng: Optional[float] = Query(default=None, alias="minLng"),
    max_lng: Optional[float] = Query(default=None, alias="maxLng"),
    compact: bool = Query(default=True),
    summary: bool = Query(default=True),
    limit: int = Query(default=DEFAULT_LIMIT),
) -> Dict[str, Any]:
    QUERY_LIST_REQUESTS_TOTAL.labels(endpoint="query").inc()
    normalized_limit = min(max(1, limit), MAX_LIMIT)

    if since and since > 0:
        updates, latest_stream_id = _collect_delta_since(since, compact=compact, summary=summary)
        updates = _filter_updates_by(
            updates,
            vehicle_id=vehicle_id,
            include_vehicle_id=include_vehicle_id,
            state=state,
            city=city,
            min_speed=min_speed,
            max_speed=max_speed,
            min_lat=min_lat,
            max_lat=max_lat,
            min_lng=min_lng,
            max_lng=max_lng,
        )

        return {
            "type": "delta",
            "count": min(len(updates), normalized_limit),
            "updates": updates[:normalized_limit],
            "stream_last_id": latest_stream_id,
        }

    vehicles = _collect_vehicles_from_latest(
        vehicle_id=vehicle_id,
        include_vehicle_id=include_vehicle_id,
        state=state,
        city=city,
        min_speed=min_speed,
        max_speed=max_speed,
        min_lat=min_lat,
        max_lat=max_lat,
        min_lng=min_lng,
        max_lng=max_lng,
        compact=compact,
        summary=summary,
    )

    return {
        "type": "snapshot",
        "count": min(len(vehicles), normalized_limit),
        "vehicles": vehicles[:normalized_limit],
        "stream_last_id": _latest_stream_id(),
    }


@app.get("/api/vehicles/watch")
def watch_vehicles(
    state: Optional[str] = Query(default=None),
    city: Optional[str] = Query(default=None),
    since: int = Query(default=0),
    vehicle_id: Optional[str] = Query(default=None),
    include_vehicle_id: Optional[str] = Query(default=None, alias="includeVehicleId"),
    min_speed: Optional[float] = Query(default=None, alias="minSpeed"),
    max_speed: Optional[float] = Query(default=None, alias="maxSpeed"),
    min_lat: Optional[float] = Query(default=None, alias="minLat"),
    max_lat: Optional[float] = Query(default=None, alias="maxLat"),
    min_lng: Optional[float] = Query(default=None, alias="minLng"),
    max_lng: Optional[float] = Query(default=None, alias="maxLng"),
    compact: bool = Query(default=True),
    summary: bool = Query(default=False),
    limit: int = Query(default=DEFAULT_LIMIT),
) -> Dict[str, Any]:
    QUERY_LIST_REQUESTS_TOTAL.labels(endpoint="watch").inc()
    """
    Conditional query endpoint:
    - since=0 -> current snapshot by filters.
    - since>0 -> incremental updates by filters.
    """
    normalized_limit = min(max(1, limit), MAX_LIMIT)

    if since and since > 0:
        updates, latest_stream_id = _collect_delta_since(since, compact=compact, summary=summary)
        updates = _filter_updates_by(
            updates,
            vehicle_id=vehicle_id,
            include_vehicle_id=include_vehicle_id,
            state=state,
            city=city,
            min_speed=min_speed,
            max_speed=max_speed,
            min_lat=min_lat,
            max_lat=max_lat,
            min_lng=min_lng,
            max_lng=max_lng,
        )

        return {
            "type": "delta",
            "count": min(len(updates), normalized_limit),
            "updates": updates[:normalized_limit],
            "stream_last_id": latest_stream_id,
        }

    vehicles = _collect_vehicles_from_latest(
        vehicle_id=vehicle_id,
        include_vehicle_id=include_vehicle_id,
        state=state,
        city=city,
        min_speed=min_speed,
        max_speed=max_speed,
        min_lat=min_lat,
        max_lat=max_lat,
        min_lng=min_lng,
        max_lng=max_lng,
        compact=compact,
        summary=summary,
    )

    return {
        "type": "snapshot",
        "count": min(len(vehicles), normalized_limit),
        "vehicles": vehicles[:normalized_limit],
        "stream_last_id": _latest_stream_id(),
    }


@app.get("/api/vehicles/compact")
def list_vehicles_compact(
    state: Optional[str] = Query(default=None),
    city: Optional[str] = Query(default=None),
    since: int = Query(default=0, alias="since"),
    vehicle_id: Optional[str] = Query(default=None),
    include_vehicle_id: Optional[str] = Query(default=None, alias="includeVehicleId"),
    min_speed: Optional[float] = Query(default=None, alias="minSpeed"),
    max_speed: Optional[float] = Query(default=None, alias="maxSpeed"),
    min_lat: Optional[float] = Query(default=None, alias="minLat"),
    max_lat: Optional[float] = Query(default=None, alias="maxLat"),
    min_lng: Optional[float] = Query(default=None, alias="minLng"),
    max_lng: Optional[float] = Query(default=None, alias="maxLng"),
    limit: int = Query(default=DEFAULT_LIMIT),
) -> Dict[str, Any]:
    QUERY_LIST_REQUESTS_TOTAL.labels(endpoint="compact").inc()
    normalized_limit = min(max(1, limit), MAX_LIMIT)
    vehicles = _collect_vehicles_from_latest(
        vehicle_id=vehicle_id,
        include_vehicle_id=include_vehicle_id,
        state=state,
        city=city,
        min_speed=min_speed,
        max_speed=max_speed,
        since_ms=since or None,
        min_lat=min_lat,
        max_lat=max_lat,
        min_lng=min_lng,
        max_lng=max_lng,
        compact=True,
    )

    return {
        "count": min(len(vehicles), normalized_limit),
        "vehicles": vehicles[:normalized_limit],
        "stream_last_id": _latest_stream_id(),
    }


@app.get("/api/vehicles/delta")
def list_vehicles_delta(
    since: int = Query(..., description="last seen event timestamp (epoch ms)"),
    vehicle_id: Optional[str] = Query(default=None),
    include_vehicle_id: Optional[str] = Query(default=None, alias="includeVehicleId"),
    state: Optional[str] = Query(default=None),
    city: Optional[str] = Query(default=None),
    compact: bool = Query(default=True),
    summary: bool = Query(default=False),
    min_speed: Optional[float] = Query(default=None, alias="minSpeed"),
    max_speed: Optional[float] = Query(default=None, alias="maxSpeed"),
    min_lat: Optional[float] = Query(default=None, alias="minLat"),
    max_lat: Optional[float] = Query(default=None, alias="maxLat"),
    min_lng: Optional[float] = Query(default=None, alias="minLng"),
    max_lng: Optional[float] = Query(default=None, alias="maxLng"),
    limit: int = Query(default=DEFAULT_LIMIT),
) -> Dict[str, Any]:
    QUERY_LIST_REQUESTS_TOTAL.labels(endpoint="delta").inc()
    updates, latest_stream_id = _collect_delta_since(since, compact=compact, summary=summary)
    updates = _filter_updates_by(
        updates,
        vehicle_id=vehicle_id,
        include_vehicle_id=include_vehicle_id,
        state=state,
        city=city,
        min_speed=min_speed,
        max_speed=max_speed,
        min_lat=min_lat,
        max_lat=max_lat,
        min_lng=min_lng,
        max_lng=max_lng,
    )

    normalized_limit = min(max(1, limit), MAX_LIMIT)

    return {
        "count": min(len(updates), normalized_limit),
        "updates": updates[:normalized_limit],
        "stream_last_id": latest_stream_id,
    }


@app.get("/api/vehicles/changes")
def list_vehicle_changes(
    since: int = Query(..., description="last seen event timestamp (epoch ms)"),
    state: Optional[str] = Query(default=None),
    city: Optional[str] = Query(default=None),
    vehicle_id: Optional[str] = Query(default=None),
    include_vehicle_id: Optional[str] = Query(default=None, alias="includeVehicleId"),
    min_speed: Optional[float] = Query(default=None, alias="minSpeed"),
    max_speed: Optional[float] = Query(default=None, alias="maxSpeed"),
    min_lat: Optional[float] = Query(default=None, alias="minLat"),
    max_lat: Optional[float] = Query(default=None, alias="maxLat"),
    min_lng: Optional[float] = Query(default=None, alias="minLng"),
    max_lng: Optional[float] = Query(default=None, alias="maxLng"),
    compact: bool = Query(default=True),
    limit: int = Query(default=DEFAULT_LIMIT),
    summary: bool = Query(default=False),
) -> Dict[str, Any]:
    QUERY_LIST_REQUESTS_TOTAL.labels(endpoint="changes").inc()
    normalized_limit = min(max(1, limit), MAX_LIMIT)
    updates, latest_stream_id = _collect_delta_since(since, compact=compact, summary=summary)
    updates = _filter_updates_by(
        updates,
        vehicle_id=vehicle_id,
        include_vehicle_id=include_vehicle_id,
        state=state,
        city=city,
        min_speed=min_speed,
        max_speed=max_speed,
        min_lat=min_lat,
        max_lat=max_lat,
        min_lng=min_lng,
        max_lng=max_lng,
    )
    if summary:
        updates = [_build_vehicle_summary(item) for item in updates]
    return {
        "type": "delta",
        "count": min(len(updates), normalized_limit),
        "updates": updates[:normalized_limit],
        "stream_last_id": latest_stream_id,
    }


@app.get("/api/vehicles/stream")
async def stream_vehicles(
    request: Request,
    compact: bool = Query(default=True),
    summary: bool = Query(default=True),
    since: int = Query(default=0),
    state: Optional[str] = Query(default=None),
    city: Optional[str] = Query(default=None),
    vehicle_id: Optional[str] = Query(default=None),
    include_vehicle_id: Optional[str] = Query(default=None, alias="includeVehicleId"),
    min_speed: Optional[float] = Query(default=None, alias="minSpeed"),
    max_speed: Optional[float] = Query(default=None, alias="maxSpeed"),
    min_lat: Optional[float] = Query(default=None, alias="minLat"),
    max_lat: Optional[float] = Query(default=None, alias="maxLat"),
    min_lng: Optional[float] = Query(default=None, alias="minLng"),
    max_lng: Optional[float] = Query(default=None, alias="maxLng"),
    heartbeat_ms: int = Query(default=1000),
    max_updates: int = Query(default=1000),
):
    QUERY_LIST_REQUESTS_TOTAL.labels(endpoint="stream").inc()
    async def _generator() -> AsyncGenerator[str, None]:
        current_since = max(0, since)
        snapshot_stream_id = _latest_stream_id()

        try:
            snapshot = _collect_vehicles_from_latest(
                vehicle_id=vehicle_id,
                include_vehicle_id=include_vehicle_id,
                state=state,
                city=city,
                min_speed=min_speed,
                max_speed=max_speed,
                min_lat=min_lat,
                max_lat=max_lat,
                min_lng=min_lng,
                max_lng=max_lng,
                compact=compact,
                summary=summary,
            )
        except Exception as exc:
            snapshot = []
            yield _sse_event(
                {
                    "type": "error",
                    "message": f"snapshot collection failed: {exc}",
                    "stream_last_id": snapshot_stream_id,
                    "received_at": int(time.time() * 1000),
                }
            )

        snapshot_payload: Dict[str, Any] = {
            "type": "snapshot",
            "count": len(snapshot),
            "vehicles": snapshot,
            "stream_last_id": snapshot_stream_id,
            "received_at": int(time.time() * 1000),
        }
        yield _sse_event(snapshot_payload)

        if snapshot_stream_id > current_since:
            current_since = snapshot_stream_id

        while True:
            if await request.is_disconnected():
                break

            try:
                updates, latest_stream_id = _collect_delta_since(current_since, compact=compact, summary=summary)
                filtered = _filter_updates_by(
                    updates,
                    vehicle_id=vehicle_id,
                    include_vehicle_id=include_vehicle_id,
                    state=state,
                    city=city,
                    min_speed=min_speed,
                    max_speed=max_speed,
                    min_lat=min_lat,
                    max_lat=max_lat,
                    min_lng=min_lng,
                    max_lng=max_lng,
                )
            except Exception as exc:
                yield _sse_event(
                    {
                        "type": "error",
                        "message": f"delta collection failed: {exc}",
                        "stream_last_id": _latest_stream_id(),
                        "received_at": int(time.time() * 1000),
                    }
                )
                await asyncio.sleep(max(0.5, heartbeat_ms / 1000))
                continue

            if filtered:
                yield _sse_event(
                    {
                        "type": "delta",
                        "count": len(filtered),
                        "updates": filtered[-max(1, max_updates):],
                        "stream_last_id": latest_stream_id,
                    }
                )
                current_since = max(current_since, latest_stream_id)
            else:
                yield _sse_event(
                    {
                        "type": "heartbeat",
                        "stream_last_id": _latest_stream_id(),
                        "received_at": int(time.time() * 1000),
                    }
                )

            await asyncio.sleep(max(0.5, heartbeat_ms / 1000))

    return StreamingResponse(
        _generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.websocket("/api/vehicles/ws")
async def websocket_vehicles(
    websocket: WebSocket,
    compact: bool = True,
    summary: bool = True,
    since: int = 0,
    state: Optional[str] = Query(default=None),
    city: Optional[str] = Query(default=None),
    vehicle_id: Optional[str] = Query(default=None),
    include_vehicle_id: Optional[str] = Query(default=None, alias="includeVehicleId"),
    min_speed: Optional[float] = Query(default=None, alias="minSpeed"),
    max_speed: Optional[float] = Query(default=None, alias="maxSpeed"),
    min_lat: Optional[float] = Query(default=None, alias="minLat"),
    max_lat: Optional[float] = Query(default=None, alias="maxLat"),
    min_lng: Optional[float] = Query(default=None, alias="minLng"),
    max_lng: Optional[float] = Query(default=None, alias="maxLng"),
):
    QUERY_WEBSOCKET_CONNECTIONS.inc()
    await websocket.accept()
    current_since = max(0, since)
    try:
        snapshot_stream_id = _latest_stream_id()
        await websocket.send_text(
            _safe_json_dumps(
                {
                    "type": "subscribed",
                    "transport": "websocket",
                    "stream_last_id": snapshot_stream_id,
                    "received_at": int(time.time() * 1000),
                }
            )
        )
        try:
            snapshot = _collect_vehicles_from_latest(
                compact=compact,
                summary=summary,
                vehicle_id=vehicle_id,
                include_vehicle_id=include_vehicle_id,
                state=state,
                city=city,
                min_speed=min_speed,
                max_speed=max_speed,
                min_lat=min_lat,
                max_lat=max_lat,
                min_lng=min_lng,
                max_lng=max_lng,
            )
        except Exception as exc:
            snapshot = []
            await websocket.send_text(
                _safe_json_dumps(
                    {
                        "type": "error",
                        "message": f"snapshot collection failed: {exc}",
                        "stream_last_id": snapshot_stream_id,
                    }
                )
            )
        await websocket.send_text(
            _safe_json_dumps(
                {
                    "type": "snapshot",
                    "count": len(snapshot),
                    "vehicles": snapshot,
                    "stream_last_id": snapshot_stream_id,
                    "received_at": int(time.time() * 1000),
                }
            )
        )

        if snapshot_stream_id > current_since:
            current_since = snapshot_stream_id

        while True:
            try:
                updates, latest_stream_id = _collect_delta_since(current_since, compact=compact, summary=summary)
                updates = _filter_updates_by(
                    updates,
                    vehicle_id=vehicle_id,
                    include_vehicle_id=include_vehicle_id,
                    state=state,
                    city=city,
                    min_speed=min_speed,
                    max_speed=max_speed,
                    min_lat=min_lat,
                    max_lat=max_lat,
                    min_lng=min_lng,
                    max_lng=max_lng,
                )
            except Exception as exc:
                await websocket.send_text(
                    _safe_json_dumps(
                        {
                            "type": "error",
                            "message": f"delta collection failed: {exc}",
                            "stream_last_id": _latest_stream_id(),
                        }
                    )
                )
                await asyncio.sleep(1)
                continue
            if updates:
                await websocket.send_text(
                    _safe_json_dumps(
                        {
                            "type": "delta",
                            "count": len(updates),
                            "updates": updates,
                            "stream_last_id": latest_stream_id,
                            "received_at": int(time.time() * 1000),
                        }
                    )
                )
                current_since = max(current_since, latest_stream_id)
            else:
                await websocket.send_text(
                    _safe_json_dumps(
                        {
                            "type": "heartbeat",
                            "stream_last_id": _latest_stream_id(),
                            "received_at": int(time.time() * 1000),
                        }
                    )
                )

            await asyncio.sleep(1)
    except WebSocketDisconnect:
        pass
    finally:
        QUERY_WEBSOCKET_CONNECTIONS.dec()
        return


@app.get("/api/vehicles/{vehicle_id}")
def get_vehicle(vehicle_id: str) -> Dict[str, Any]:
    QUERY_DETAIL_REQUESTS_TOTAL.labels(endpoint="vehicle").inc()
    key = f"vehicle:{vehicle_id}:latest"
    try:
        raw_value = redis_client.get(key)
    except Exception as exc:
        QUERY_REDIS_FAILURES_TOTAL.labels(operation="vehicle_get").inc()
        raise HTTPException(status_code=503, detail=f"redis read failed: {exc}")
    if not raw_value:
        raise HTTPException(status_code=404, detail="vehicle not found")

    payload = _safe_json_loads(raw_value)
    if payload is None:
        raise HTTPException(status_code=500, detail="invalid vehicle payload")

    return _build_full_vehicle(payload if isinstance(payload, dict) else {})


@app.get("/api/vehicles/{vehicle_id}/history")
def get_vehicle_history(
    vehicle_id: str,
    since: int = Query(default=0, alias="since"),
    limit: int = Query(default=20),
) -> Dict[str, Any]:
    QUERY_DETAIL_REQUESTS_TOTAL.labels(endpoint="history").inc()
    max_limit = min(max(1, limit), 500)

    try:
        entries = redis_client.xrevrange(VEHICLE_UPDATE_STREAM, count=1000)
    except Exception:
        QUERY_REDIS_FAILURES_TOTAL.labels(operation="history").inc()
        return {"vehicle_id": vehicle_id, "count": 0, "history": []}

    history: List[Dict[str, Any]] = []
    for _, fields in entries:
        if not isinstance(fields, dict):
            continue
        if fields.get("vehicle_id") != vehicle_id:
            continue

        event_ts = _to_int(fields.get("event_ts"))
        if since and event_ts <= since:
            continue

        raw_snapshot = fields.get("v")
        payload = _safe_json_loads(raw_snapshot)
        if payload is None and isinstance(raw_snapshot, dict):
            payload = raw_snapshot

        if not isinstance(payload, dict):
            continue

        snapshot = _build_vehicle_snapshot(payload if isinstance(payload, dict) else {})
        snapshot["event_ts"] = event_ts
        history.append(snapshot)

        if len(history) >= max_limit:
            break

    history.sort(key=lambda item: _to_int(item.get("event_ts")))
    return {
        "vehicle_id": vehicle_id,
        "count": len(history),
        "history": history,
    }


if __name__ == "__main__":
    import uvicorn

    host = os.getenv("QUERY_API_HOST", "0.0.0.0")
    port = int(os.getenv("QUERY_API_PORT", "8000"))
    uvicorn.run(app, host=host, port=port, ws="websockets")
