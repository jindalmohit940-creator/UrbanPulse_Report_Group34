"""
UrbanPulse - Priority consumer architecture on urbanpulse.traffic_signals
(Task B, problem 6)

WHY THIS VERSION USES confluent-kafka INSTEAD OF kafka-python-ng
--------------------------------------------------------------------
Every earlier version crashed with:
    ValueError: Invalid file descriptor: -1
Testing eliminated concurrency as the cause: a single, fully isolated
consumer (its own OS process, nothing else running) crashed on its very
FIRST poll. That means the bug isn't about consumers interfering with each
other -- it's kafka-python-ng's pure-Python socket/selector handling being
broken with this Python 3.13 + Windows combination, full stop.

The fix: use confluent-kafka-python instead. It wraps Kafka's official C
client (librdkafka) rather than using Python's `selectors` module at all,
so this bug class cannot occur.

INSTALL FIRST:
    pip install confluent-kafka

Two independent consumer groups reading the SAME topic:
  HIGH_PRIORITY (group="traffic-signals-high-priority")
    - 1 consumer, all partitions. Must keep near-zero lag.
  STANDARD_PRIORITY (group="traffic-signals-standard-priority")
    - 3 consumers, artificially slowed, to simulate falling behind.

Lag is read via `kafka-consumer-groups --describe` (through docker exec),
Kafka's own CLI -- ground truth, independent of whichever client library the
consumers use.

Run: python priority_consumer_demo.py
(Run traffic_signals_producer.py first, or concurrently, to have data flowing.)
"""
import json
import multiprocessing
import os
import subprocess
import sys
import time

from confluent_kafka import Consumer

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from config.kafka_config import (
    BOOTSTRAP_SERVERS,
    TOPIC_TRAFFIC_SIGNALS,
    CONSUMER_GROUP_HIGH_PRIORITY,
    CONSUMER_GROUP_STANDARD_PRIORITY,
)

RUN_SECONDS = 45
STANDARD_ARTIFICIAL_DELAY_SEC = 0.25  # simulates a slow analytics consumer
LAG_SAMPLE_INTERVAL_SEC = 10

KAFKA_CONTAINER = "kafka-1"
KAFKA_INTERNAL_BOOTSTRAP = "kafka-1:29092"

# confluent-kafka wants a single comma-separated string, not a list.
BOOTSTRAP_SERVERS_STR = ",".join(BOOTSTRAP_SERVERS)


def group_lag_via_cli(group_id):
    try:
        result = subprocess.run(
            [
                "docker", "exec", KAFKA_CONTAINER,
                "kafka-consumer-groups",
                "--bootstrap-server", KAFKA_INTERNAL_BOOTSTRAP,
                "--describe",
                "--group", group_id,
            ],
            capture_output=True,
            text=True,
            timeout=15,
        )
        total_lag = 0
        found_row = False
        for line in result.stdout.splitlines():
            parts = line.split()
            if len(parts) >= 6 and parts[0] == group_id and parts[2].isdigit():
                lag_str = parts[5]
                if lag_str.isdigit():
                    total_lag += int(lag_str)
                    found_row = True
        return total_lag if found_row else None
    except Exception as e:
        print(f"[lag] skipped a reading due to: {e}")
        return None


def _consumer_process_main(group_id, group_instance_id, label, delay_sec, run_seconds, count_queue):
    """Entry point for a consumer running in its OWN process, using
    confluent-kafka (librdkafka) instead of kafka-python-ng."""
    conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS_STR,
        "group.id": group_id,
        "group.instance.id": group_instance_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 1000,
    }
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC_TRAFFIC_SIGNALS])

    count = 0
    deadline = time.time() + run_seconds
    try:
        while time.time() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[{label}] consumer error, continuing: {msg.error()}")
                continue
            json.loads(msg.value().decode("utf-8"))  # simulate real deserialization
            if delay_sec:
                time.sleep(delay_sec)
            count += 1
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        count_queue.put((label, count))
        print(f"[{label}] processed {count} messages")


def main():
    print("Taking BASELINE lag reading before starting consumers...")
    baseline_high = group_lag_via_cli(CONSUMER_GROUP_HIGH_PRIORITY)
    baseline_std = group_lag_via_cli(CONSUMER_GROUP_STANDARD_PRIORITY)
    print(f"[baseline] HIGH_PRIORITY={baseline_high}  STANDARD_PRIORITY={baseline_std}\n")

    count_queue = multiprocessing.Queue()

    procs = [
        multiprocessing.Process(
            target=_consumer_process_main,
            args=(CONSUMER_GROUP_HIGH_PRIORITY, "high-priority-static-1", "HIGH_PRIORITY",
                  0.0, RUN_SECONDS, count_queue),
            name="HIGH_PRIORITY",
        ),
    ]
    for i in (1, 2, 3):
        procs.append(multiprocessing.Process(
            target=_consumer_process_main,
            args=(CONSUMER_GROUP_STANDARD_PRIORITY, f"standard-static-{i}", f"STANDARD_PRIORITY-{i}",
                  STANDARD_ARTIFICIAL_DELAY_SEC, RUN_SECONDS, count_queue),
            name=f"STD-{i}",
        ))

    for p in procs:
        p.start()

    lag_log = []
    t0 = time.time()
    while time.time() - t0 < RUN_SECONDS:
        time.sleep(LAG_SAMPLE_INTERVAL_SEC)
        high_lag = group_lag_via_cli(CONSUMER_GROUP_HIGH_PRIORITY)
        std_lag = group_lag_via_cli(CONSUMER_GROUP_STANDARD_PRIORITY)
        lag_log.append((time.time() - t0, high_lag, std_lag))
        print(f"[lag] HIGH_PRIORITY={high_lag}  STANDARD_PRIORITY={std_lag}")

    for p in procs:
        p.join(timeout=20)

    print("\nTaking FINAL lag reading after stopping consumers...")
    final_high = group_lag_via_cli(CONSUMER_GROUP_HIGH_PRIORITY)
    final_std = group_lag_via_cli(CONSUMER_GROUP_STANDARD_PRIORITY)

    counts = {}
    while not count_queue.empty():
        label, count = count_queue.get()
        counts[label] = count
    print("\n=== Message counts ===")
    for label, count in counts.items():
        print(f"{label}: {count}")

    print("\n=== Lag summary over time (ground truth via kafka-consumer-groups CLI) ===")
    print(f"{'time':>8}  {'HIGH_PRIORITY':>14}  {'STANDARD_PRIORITY':>18}")
    print(f"{'baseline':>8}  {str(baseline_high):>14}  {str(baseline_std):>18}")
    for ts, high_lag, std_lag in lag_log:
        print(f"{ts:8.1f}  {str(high_lag):>14}  {str(std_lag):>18}")
    print(f"{'final':>8}  {str(final_high):>14}  {str(final_std):>18}")

    if final_high is not None and final_std is not None:
        print(f"\nHIGH_PRIORITY lag ended at {final_high} while STANDARD_PRIORITY "
              f"still had {final_std} messages of lag remaining -- different "
              f"consumer groups track offsets independently, so STANDARD_PRIORITY's "
              f"artificial slowdown never throttled HIGH_PRIORITY.")


if __name__ == "__main__":
    main()