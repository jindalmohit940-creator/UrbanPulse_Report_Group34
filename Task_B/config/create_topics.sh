#!/bin/bash
# UrbanPulse Kafka topic setup
# Bootstrap servers reachable from host machine (docker-compose port mappings)
BOOTSTRAP="localhost:9092,localhost:9094,localhost:9096"
KAFKA_BIN="docker exec kafka-1 kafka-topics"

create_topic () {
  local name=$1
  local partitions=$2
  local retention_ms=$3
  echo "Creating topic: $name (partitions=$partitions, retention_ms=$retention_ms)"
  $KAFKA_BIN --bootstrap-server $BOOTSTRAP --create --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor 3 \
    --config retention.ms="$retention_ms" \
    --config min.insync.replicas=2
}

# --- urbanpulse.bus_gps ---
# ~8000 events / (assume 8hr sample window) -> ~0.28 events/sec/route across 50 routes.
# Keyed by route_id for per-route ordering (req. 5). 6 partitions: comfortably covers
# 50 routes hashed across partitions while giving Flink (speed layer, ETA enrichment)
# and Spark batch consumers enough parallelism headroom without over-partitioning a
# modest-throughput stream. Retention 24h -> accident investigation replay window.
create_topic "urbanpulse.bus_gps" 6 86400000

# --- urbanpulse.traffic_signals ---
# ~2500 events across junctions. Must be >=3 partitions so STANDARD_PRIORITY (3 consumers)
# can each own a partition and parallelize independently of HIGH_PRIORITY (1 consumer,
# reads all partitions via a single-consumer group). We pick 6 partitions: divisible by
# both 1 and 3, giving STANDARD_PRIORITY consumers 2 partitions each (balanced) while
# HIGH_PRIORITY's single consumer still reads all 6 with low per-partition backlog.
# Retention: not specified in the assignment -> we justify 7 days. Traffic signal control
# is a real-time concern (speed layer), but 7 days supports short-horizon congestion
# trend analysis and debugging signal-timing incidents without the storage cost of a
# long retention like air_quality/smart_meters, which serve regulatory/trend use cases.
create_topic "urbanpulse.traffic_signals" 6 604800000

# --- urbanpulse.air_quality ---
# ~1500 events, low-moderate throughput. Retention 90 days -> pollution trend analysis
# (Task A: environmental reporting / seasonal AQI trend requirement). 3 partitions is
# enough for Flink real-time AQI alerting + Spark batch trend jobs to parallelize; no
# ordering-key requirement stated so round-robin/sensor_id keying across 3 partitions
# is sufficient for this volume.
create_topic "urbanpulse.air_quality" 3 7776000000

# --- urbanpulse.smart_meters ---
# ~4000 events/day-ish from meters across wards. Retention 365 days -> regulatory
# energy audit requirement (directly supports the councillor/audit reporting matrix
# from Task A). 4 partitions: moderate throughput, batch-heavy consumption pattern
# (Spark serving layer for billing/audit), doesn't need HIGH_PRIORITY-style parallelism.
create_topic "urbanpulse.smart_meters" 4 31536000000

# --- urbanpulse.dlq ---
# Dead-letter queue for records failing validation across all 4 streams. Single topic,
# 3 partitions (parallel DLQ consumers for the 5-min error-distribution report), long-ish
# retention (30 days) so ops/data-quality teams have time to investigate and reprocess.
create_topic "urbanpulse.dlq" 3 2592000000

echo "Done. Listing topics:"
docker exec kafka-1 kafka-topics --bootstrap-server $BOOTSTRAP --list
