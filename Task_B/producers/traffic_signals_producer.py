"""
UrbanPulse - traffic_signals producer. Feeds urbanpulse.traffic_signals so the
HIGH_PRIORITY / STANDARD_PRIORITY consumer demo (Task B, problem 6) has data
to consume. Keyed by junction_id (stable per-junction ordering, useful if a
future consumer needs to track one junction's phase transitions in order).
"""
import json
import os
import sys

from kafka import KafkaProducer

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from config.kafka_config import BOOTSTRAP_SERVERS, TOPIC_TRAFFIC_SIGNALS, TOPIC_DLQ
from dlq.validators import validate_traffic_signals, wrap_dlq_record

DATA_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "traffic_signals_sample.jsonl")


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
            is_valid, error_reason = validate_traffic_signals(rec)
            if not is_valid:
                producer.send(TOPIC_DLQ, key=str(rec.get("junction_id", "unknown")),
                               value=wrap_dlq_record(TOPIC_TRAFFIC_SIGNALS, rec, error_reason))
                dlq_count += 1
                continue
            producer.send(TOPIC_TRAFFIC_SIGNALS, key=rec["junction_id"], value=rec)
            sent += 1
    producer.flush()
    producer.close()
    print(f"traffic_signals_producer: sent={sent} routed_to_dlq={dlq_count}")


if __name__ == "__main__":
    run()
