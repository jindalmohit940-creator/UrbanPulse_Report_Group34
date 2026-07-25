"""
UrbanPulse - DLQ 5-minute error distribution report (Task B, problem 8)

Consumes urbanpulse.dlq for a 5-minute window and reports error_reason
distribution, broken down by source_topic, so data-quality/ops teams can
spot which stream/rule is generating the most bad data.
"""
import json
import os
import sys
import time
from collections import Counter, defaultdict

from kafka import KafkaConsumer

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from config.kafka_config import BOOTSTRAP_SERVERS, TOPIC_DLQ

REPORT_WINDOW_SEC = 300  # 5 minutes

VALIDATION_RULES = {
    "urbanpulse.bus_gps": [
        ("IMPOSSIBLE_GPS_COORDINATES", "lat/lon outside valid Earth range"),
        ("SPEED_OUT_OF_RANGE", "speed_kmh outside 0-120 (unsafe for road traffic)"),
        ("OCCUPANCY_OUT_OF_RANGE", "occupancy_pct outside 0-100%"),
    ],
    "urbanpulse.air_quality": [
        ("NULL_AQI", "aqi field is null/missing"),
    ],
    "urbanpulse.smart_meters": [
        ("VOLTAGE_OUT_OF_RANGE", "voltage outside 180-260V (brownout/spike)"),
        ("NEGATIVE_KWH_READING", "kwh_reading is negative"),
    ],
}

def print_validation_rules():
    print("=== Validation Rules Applied ===")
    for topic, rules in VALIDATION_RULES.items():
        print(f"  {topic}")
        for reason, desc in rules:
            print(f"    {reason:<28} -> {desc}")
    print()
def run(window_sec=REPORT_WINDOW_SEC):
    consumer = KafkaConsumer(
        TOPIC_DLQ,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="dlq-report-job",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=5000,
    )

    error_counts = Counter()
    per_topic_errors = defaultdict(Counter)
    total = 0
    start = time.time()

    print(f"Collecting DLQ records for up to {window_sec}s...")
    while time.time() - start < window_sec:
        for msg in consumer:
            rec = msg.value
            reason = rec.get("error_reason", "UNKNOWN")
            source = rec.get("source_topic", "UNKNOWN")
            error_counts[reason] += 1
            per_topic_errors[source][reason] += 1
            total += 1
            if time.time() - start >= window_sec:
                break
        if consumer._closed:
            break
        # consumer_timeout_ms means the for-loop above exits when no new
        # messages arrive for 5s; loop back and keep waiting until window ends.

    consumer.close()
    print_validation_rules()
    print("\n=== DLQ 5-Minute Error Distribution Report ===")
    print(f"Window: {window_sec}s | Total DLQ records: {total}\n")
    print("By error_reason:")
    for reason, count in error_counts.most_common():
        pct = (count / total * 100) if total else 0
        print(f"  {reason:<30} {count:>6}  ({pct:5.1f}%)")

    print("\nBy source_topic -> error_reason:")
    for topic, counter in per_topic_errors.items():
        print(f"  {topic}")
        for reason, count in counter.most_common():
            print(f"    {reason:<28} {count:>6}")


if __name__ == "__main__":
    run()
