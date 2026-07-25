"""
UrbanPulse - bus_gps x route_schedule enrichment (Task B, problem 7)

Joins the urbanpulse.bus_gps stream with route_schedule.csv, modeled as a
static KTable keyed by route_id (the join key, matching the bus_gps message
key set in bus_gps_producer.py -- this is what makes a stream-table join
efficient in real Kafka Streams: same key, same partitioning).

Output: an enriched stream written to urbanpulse.bus_eta_enriched containing
GPS position + scheduled_arrival_time + route_name + terminal. This is
explicitly the foundation for the real-time ETA service flagged as a pain
point in Task A (riders currently have no reliable arrival estimate).

Implementation note: this uses the kafka-python client with an in-memory
dict as the KTable, which mirrors what Kafka Streams' internal state store
does under the hood (a compacted, in-memory/RocksDB-backed table keyed by
route_id). For the actual submission this logic maps directly onto Kafka
Streams DSL:

    KStream<String, BusGps> busGps = builder.stream("urbanpulse.bus_gps");
    KTable<String, RouteSchedule> routeSchedule =
        builder.table("urbanpulse.route_schedule");  // or loaded via GlobalKTable
    KStream<String, EnrichedBusPosition> enriched =
        busGps.join(routeSchedule, (gps, sched) -> new EnrichedBusPosition(gps, sched));
    enriched.to("urbanpulse.bus_eta_enriched");

which is what this script emulates end-to-end and produces real output for.
"""
import csv
import json
import os
import sys

from kafka import KafkaConsumer, KafkaProducer

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from config.kafka_config import BOOTSTRAP_SERVERS, TOPIC_BUS_GPS

TOPIC_ENRICHED = "urbanpulse.bus_eta_enriched"
ROUTE_SCHEDULE_CSV = os.path.join(os.path.dirname(__file__), "..", "data", "route_schedule.csv")


def load_route_schedule_ktable(path=ROUTE_SCHEDULE_CSV):
    """Loads route_schedule.csv into an in-memory dict keyed by route_id --
    the KTable equivalent. In production this table refreshes from its
    changelog topic; here it's loaded once at startup since the schedule is
    near-static reference data."""
    table = {}
    with open(path) as f:
        for row in csv.DictReader(f):
            table[row["route_id"]] = {
                "route_name": row["route_name"],
                "terminal": row["terminal"],
                "scheduled_arrival_time": row["scheduled_arrival_time"],
            }
    return table


def run(consume_timeout_ms=10000, limit=None):
    route_table = load_route_schedule_ktable()
    print(f"Loaded route_schedule KTable: {len(route_table)} routes")

    consumer = KafkaConsumer(
        TOPIC_BUS_GPS,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="eta-enrichment-job",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        consumer_timeout_ms=consume_timeout_ms,
    )
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )

    joined, unmatched = 0, 0
    for msg in consumer:
        if limit and joined + unmatched >= limit:
            break
        gps = msg.value
        route_id = msg.key or gps.get("route_id")
        sched = route_table.get(route_id)

        if sched is None:
            # Stream-table join miss: route not found in the KTable.
            unmatched += 1
            continue

        enriched = {
            "bus_id": gps["bus_id"],
            "route_id": route_id,
            "lat": gps["lat"],
            "lon": gps["lon"],
            "speed_kmh": gps["speed_kmh"],
            "occupancy_pct": gps["occupancy_pct"],
            "gps_timestamp": gps["timestamp"],
            "route_name": sched["route_name"],
            "terminal": sched["terminal"],
            "scheduled_arrival_time": sched["scheduled_arrival_time"],
        }
        producer.send(TOPIC_ENRICHED, key=route_id, value=enriched)
        joined += 1

    producer.flush()
    producer.close()
    consumer.close()
    print(f"eta_enrichment: joined={joined} unmatched={unmatched} -> topic={TOPIC_ENRICHED}")


if __name__ == "__main__":
    run()
