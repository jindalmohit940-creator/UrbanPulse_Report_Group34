"""
UrbanPulse - smart_meters producer. Feeds urbanpulse.smart_meters (365-day
retention, regulatory energy audit use case). Keyed by ward_id so Spark's
batch/serving layer can aggregate per-ward consumption efficiently.
"""
import json
import os
import sys

from kafka import KafkaProducer

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from config.kafka_config import BOOTSTRAP_SERVERS, TOPIC_SMART_METERS, TOPIC_DLQ
from dlq.validators import validate_smart_meters, wrap_dlq_record

DATA_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "smart_meters_sample.jsonl")


def run(limit=None):
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
    )
    sent, dlq_count = 0, 0
    with open(DATA_FILE) as f:
        for i, line in enumerate(f):
            if limit and i >= limit:
                break
            rec = json.loads(line)
            is_valid, error_reason = validate_smart_meters(rec)
            if not is_valid:
                producer.send(TOPIC_DLQ, key=str(rec.get("ward_id", "unknown")),
                               value=wrap_dlq_record(TOPIC_SMART_METERS, rec, error_reason))
                dlq_count += 1
                continue
            producer.send(TOPIC_SMART_METERS, key=rec["ward_id"], value=rec)
            sent += 1
    producer.flush()
    producer.close()
    print(f"smart_meters_producer: sent={sent} routed_to_dlq={dlq_count}")


if __name__ == "__main__":
    run()
