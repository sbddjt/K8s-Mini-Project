import json
import os
import signal
from datetime import datetime, timezone

from kafka import KafkaConsumer
from kafka.errors import KafkaError
from pymongo import MongoClient
from pymongo.errors import BulkWriteError, OperationFailure
from prometheus_client import Counter, Gauge, Histogram, start_http_server

RUNNING = True
MONGO_CONSUMER_MESSAGES_TOTAL = Counter(
    "mongo_consumer_messages_total",
    "Total Kafka messages processed by the MongoDB consumer.",
)
MONGO_CONSUMER_BATCHES_TOTAL = Counter(
    "mongo_consumer_batches_total",
    "Total MongoDB consumer batches attempted.",
)
MONGO_INSERT_FAILURES_TOTAL = Counter(
    "mongo_insert_failures_total",
    "Total MongoDB consumer insert failures.",
)
MONGO_BATCH_INSERT_SECONDS = Histogram(
    "mongo_batch_insert_seconds",
    "Latency of MongoDB batch inserts from the command consumer.",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)
MONGO_CONSUMER_UP = Gauge(
    "mongo_consumer_up",
    "Whether the MongoDB consumer main loop is running.",
)


def _handle_sigterm(signum, frame):
    global RUNNING
    print("\n[INFO] Shutdown signal received. Exiting...")
    RUNNING = False


signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)


def _ensure_ttl_index(db, collection, collection_name: str, ttl_seconds: int):
    index_name = "telemetry_created_at_ttl"

    try:
        collection.create_index(
            "created_at",
            expireAfterSeconds=ttl_seconds,
            name=index_name,
        )
        return
    except OperationFailure as exc:
        message = str(exc)
        if "IndexOptionsConflict" not in message and "already exists with different options" not in message:
            raise

    db.command(
        {
            "collMod": collection_name,
            "index": {
                "name": index_name,
                "expireAfterSeconds": ttl_seconds,
            },
        }
    )


def start_consumer():
    # Runtime config
    topic = os.getenv("KAFKA_TOPIC", "car.telemetry.events")
    bootstrap = os.getenv("KAFKA_BROKERS", os.getenv("KAFKA_BOOTSTRAP", "kafka-svc:9092"))
    group_id = os.getenv("KAFKA_GROUP_ID", "vehicle-mongo-saver")
    batch_size = int(os.getenv("MONGO_BATCH_SIZE", "500"))
    metrics_port = int(os.getenv("METRICS_PORT", "9101"))

    mongo_uri = os.getenv("MONGO_URI", "mongodb://mongo-svc:27017")
    mongo_db_name = os.getenv("MONGO_DB", "car_db")
    mongo_col_name = os.getenv("MONGO_COLLECTION", "telemetry_history")

    mongo_client = None
    consumer = None

    start_http_server(metrics_port)
    print(f"[INFO] MongoDB consumer metrics exposed on :{metrics_port}/metrics")

    try:
        mongo_client = MongoClient(mongo_uri)
        db = mongo_client[mongo_db_name]
        collection = db[mongo_col_name]
        mongo_client.admin.command("ping")

        ttl_minutes = int(os.getenv("MONGO_TTL_MINUTES", "0"))
        ttl_days = int(os.getenv("MONGO_TTL_DAYS", "7"))
        ttl_seconds = ttl_minutes * 60 if ttl_minutes > 0 else ttl_days * 24 * 60 * 60
        _ensure_ttl_index(db, collection, mongo_col_name, ttl_seconds)
        print(f"[INFO] MongoDB connected: {mongo_db_name}.{mongo_col_name}")
    except Exception as e:
        print(f"[ERROR] MongoDB connection failed: {e}")
        return

    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap.split(","),
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )
        print(f"[INFO] Kafka -> MongoDB consumer started (Topic: {topic}, Group: {group_id})")
    except Exception as e:
        print(f"[ERROR] Kafka connection failed: {e}")
        if mongo_client is not None:
            mongo_client.close()
        return

    try:
        MONGO_CONSUMER_UP.set(1)
        while RUNNING:
            records = consumer.poll(timeout_ms=1000, max_records=batch_size)

            if not records:
                continue

            batch_documents = []
            for _, msgs in records.items():
                for msg in msgs:
                    try:
                        vehicle_data = msg.value
                        now = datetime.now(timezone.utc)
                        vehicle_data["created_at"] = now
                        vehicle_data["_consumed_at"] = now

                        vehicle_id = vehicle_data.get("vehicle_id")
                        if not vehicle_id:
                            print(f"[WARN] missing vehicle_id: {vehicle_data}")
                            continue

                        batch_documents.append(vehicle_data)
                    except Exception as e:
                        print(f"[ERROR] message parse failed: {e}")

            if not batch_documents:
                continue

            try:
                MONGO_CONSUMER_MESSAGES_TOTAL.inc(len(batch_documents))
                MONGO_CONSUMER_BATCHES_TOTAL.inc()
                with MONGO_BATCH_INSERT_SECONDS.time():
                    collection.insert_many(batch_documents)
                consumer.commit()
                print(f"[INFO] MongoDB insert + Kafka offset commit: {len(batch_documents)} rows")
            except BulkWriteError as bwe:
                MONGO_INSERT_FAILURES_TOTAL.inc()
                print(f"[ERROR] MongoDB bulk write failed: {bwe.details}")
            except Exception as e:
                MONGO_INSERT_FAILURES_TOTAL.inc()
                print(f"[ERROR] MongoDB write failed: {e}")

    except KafkaError as e:
        print(f"[ERROR] Kafka error: {e}")
    finally:
        MONGO_CONSUMER_UP.set(0)
        if consumer is not None:
            consumer.close()
        if mongo_client is not None:
            mongo_client.close()
        print("[INFO] Kafka Consumer & MongoDB stopped")


if __name__ == "__main__":
    start_consumer()
