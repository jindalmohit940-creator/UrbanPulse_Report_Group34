# UrbanPulse — Task B: Kafka Ingestion Layer

DSE ZG556, Group 52. Lambda architecture ingestion backbone: this Kafka layer
feeds both the Flink speed layer and the Spark Structured Streaming batch/serving
layer (Task A/C).

## Structure
```
docker-compose.yml              3-broker Kafka cluster (KRaft, + Kafka UI on :8080)
config/
  create_topics.sh              Topic creation w/ justified partitions & retention
  kafka_config.py                Shared bootstrap servers / topic / group names
data/
  generate_dummy_data.py         Generates all sample input files (already run)
  *.csv / *.jsonl                Dummy data matching assignment schemas
producers/
  bus_gps_producer.py            Problem 5: keyed by route_id, DLQ routing
  air_quality_producer.py        Problem 5: at-least-once, retries, null AQI handling
  traffic_signals_producer.py    Feeds problem 6 demo
  smart_meters_producer.py       Feeds the 365-day audit topic
consumers/
  priority_consumer_demo.py      Problem 6: HIGH vs STANDARD priority groups + lag proof
streams/
  eta_enrichment.py              Problem 7: bus_gps stream x route_schedule KTable join
dlq/
  validators.py                  Validation rules (3+ per stream) used by all producers
  dlq_report.py                  Problem 8: 5-minute DLQ error distribution report
```

## Running it end to end

```bash
# 1. Bring up the cluster
docker compose up -d
sleep 15   # let brokers elect controller/finish startup

# 2. Create topics
chmod +x config/create_topics.sh
./config/create_topics.sh

# 3. Install client deps
pip install -r requirements.txt

# 4. Produce data (run from task_b/ root)
python3 producers/bus_gps_producer.py
python3 producers/air_quality_producer.py
python3 producers/traffic_signals_producer.py
python3 producers/smart_meters_producer.py

# 5. Run the priority consumer demo (problem 6) — proves HIGH_PRIORITY lag
#    stays near-zero while STANDARD_PRIORITY is artificially slowed.
#    Re-run traffic_signals_producer.py in another terminal while this runs
#    if you want a longer sustained feed.
python3 consumers/priority_consumer_demo.py

# 6. Run the ETA enrichment stream-table join (problem 7)
python3 streams/eta_enrichment.py

# 7. Generate the DLQ error report (problem 8)
python3 dlq/dlq_report.py
```

## Design decisions (full justification also in create_topics.sh comments)

| Topic | Partitions | Retention | Why |
|---|---|---|---|
| `urbanpulse.bus_gps` | 6 | 24h | Keyed by route_id for ordering; 24h supports accident-investigation replay |
| `urbanpulse.traffic_signals` | 6 | 7d | Divisible by both 1 (HIGH_PRIORITY) and 3 (STANDARD_PRIORITY) consumers; 7d balances real-time control vs. short-horizon congestion trend debugging |
| `urbanpulse.air_quality` | 3 | 90d | Low-moderate throughput; 90d for pollution trend analysis |
| `urbanpulse.smart_meters` | 4 | 365d | Ties to Task A's regulatory/councillor audit requirement |
| `urbanpulse.dlq` | 3 | 30d | Parallel DLQ report consumption; enough time for reprocessing |

## Notes for the report
- `traffic_signals` retention (7 days) was not specified in the assignment; the
  justification is written inline in `create_topics.sh` and above.
- The priority consumer demo proves the near-zero-lag requirement empirically
  by sampling consumer group lag every 5s for 45s while STANDARD_PRIORITY
  processing is artificially delayed — see printed lag table + summary at the
  end of `priority_consumer_demo.py`'s output.
- `eta_enrichment.py`'s docstring includes the equivalent real Kafka Streams
  DSL (Java) for the report, since the assignment's stack is Java-based Kafka
  Streams — this Python version is a directly runnable stand-in/demo using
  the same dummy data.
