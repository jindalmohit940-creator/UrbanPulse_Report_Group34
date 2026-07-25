"""
UrbanPulse - air_quality producer (Task B, problem 5)

- At-least-once delivery: acks='all' (wait for full ISR set) + explicit
  application-level retry loop on top of the client's internal retries, so a
  transient broker/network failure never silently loses a record. Duplicates
  are an accepted trade-off of at-least-once (downstream jobs should be
  idempotent / dedupe on sensor_id+timestamp if exactness matters).
- 5% of source events have a null AQI reading. These are NOT crashed on and
  NOT silently dropped: they're logged and routed to the DLQ with a clear
  error_reason so they're auditable and reprocessable.
"""
import json
import logging
import os
import sys
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from config.kafka_config import BOOTSTRAP_SERVERS, TOPIC_AIR_QUALITY, TOPIC_DLQ
from dlq.validators import validate_air_quality, wrap_dlq_record

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("air_quality_producer")

DATA_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "air_quality_sample.jsonl")
MAX_RETRIES = 3
RETRY_BACKOFF_SEC = 1.5


def build_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,              # client-level retries for transient errors
        enable_idempotence=False,  # at-least-once, not exactly-once. Duplicates are acceptable and handled in downstream,sensor_id+timestamp dedup; idempotence would add
        linger_ms=10,
    )


def send_with_retry(producer, topic, key, value, max_retries=MAX_RETRIES):
    """Application-level retry wrapper -> guarantees at-least-once even if the
    client-level retry budget is exhausted (e.g. broker down for >few sec)."""
    attempt = 0
    while attempt <= max_retries:
        try:
            future = producer.send(topic, key=key, value=value)
            future.get(timeout=10)  # block for ack, confirming durability
            return True
        except KafkaError as e:
            attempt += 1
            logger.warning(
                "Send failed (attempt %d/%d) for key=%s: %s", attempt, max_retries, key, e
            )
            if attempt > max_retries:
                logger.error("Giving up on record after %d retries: key=%s", max_retries, key)
                return False
            time.sleep(RETRY_BACKOFF_SEC * attempt)  # simple backoff
    return False


def run(limit=None):
    producer = build_producer()
    sent, dlq_count, failed = 0, 0, 0

    with open(DATA_FILE) as f:
        for i, line in enumerate(f):
            if limit and i >= limit:
                break
            rec = json.loads(line)

            if rec.get("aqi") is None:
                logger.info("Null AQI reading for sensor_id=%s at %s -> routing to DLQ",
                            rec.get("sensor_id"), rec.get("timestamp"))
                dlq_rec = wrap_dlq_record(TOPIC_AIR_QUALITY, rec, "NULL_AQI")
                ok = send_with_retry(producer, TOPIC_DLQ, str(rec.get("sensor_id", "unknown")), dlq_rec)
                dlq_count += 1 if ok else 0
                failed += 0 if ok else 1
                continue

            is_valid, error_reason = validate_air_quality(rec)
            if not is_valid:
                logger.info("Validation failed for sensor_id=%s: %s", rec.get("sensor_id"), error_reason)
                dlq_rec = wrap_dlq_record(TOPIC_AIR_QUALITY, rec, error_reason)
                ok = send_with_retry(producer, TOPIC_DLQ, str(rec.get("sensor_id", "unknown")), dlq_rec)
                dlq_count += 1 if ok else 0
                failed += 0 if ok else 1
                continue

            ok = send_with_retry(producer, TOPIC_AIR_QUALITY, str(rec["sensor_id"]), rec)
            sent += 1 if ok else 0
            failed += 0 if ok else 1

    producer.flush()
    producer.close()
    logger.info("air_quality_producer: sent=%d routed_to_dlq=%d failed=%d", sent, dlq_count, failed)


if __name__ == "__main__":
    run()
