"""
UrbanPulse - bus_gps producer (Task B, problem 5)

- Keys every message by route_id -> guarantees per-route ordering, since Kafka
  preserves order only within a partition, and same-key records always land
  on the same partition.
- Reads from data/bus_gps_sample.jsonl.
- Invalid records (impossible GPS coords, bad speed/occupancy) go to the DLQ
  instead of being silently dropped or crashing the producer.
"""
import json
import os
import sys
import time

from kafka import KafkaProducer

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from config.kafka_config import BOOTSTRAP_SERVERS, TOPIC_BUS_GPS, TOPIC_DLQ
from dlq.validators import validate_bus_gps, wrap_dlq_record

DATA_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "bus_gps_sample.jsonl")


def build_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",  # durability: wait for all in-sync replicas
        retries=5,
        linger_ms=10,
    )


def run(limit=None, sleep_between=0.0):
    producer = build_producer()
    sent, dlq_count = 0, 0

    with open(DATA_FILE) as f:
        for i, line in enumerate(f):
            if limit and i >= limit:
                break
            rec = json.loads(line)

            is_valid, error_reason = validate_bus_gps(rec)
            if not is_valid:
                dlq_rec = wrap_dlq_record(TOPIC_BUS_GPS, rec, error_reason)
                producer.send(TOPIC_DLQ, key=str(rec.get("route_id", "unknown")), value=dlq_rec)
                dlq_count += 1
                continue

            producer.send(TOPIC_BUS_GPS, key=rec["route_id"], value=rec)
            sent += 1

            if sleep_between:
                time.sleep(sleep_between)

    producer.flush()
    producer.close()
    print(f"bus_gps_producer: sent={sent} routed_to_dlq={dlq_count}")


if __name__ == "__main__":
    run()
