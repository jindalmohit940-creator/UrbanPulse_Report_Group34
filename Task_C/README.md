# UrbanPulse — Task C: Flink Incident Detection & Spark Urban Analytics Engine

DSE ZG556, Group 34. Speed layer (Flink) + batch/serving layer (Spark
Structured Streaming) of the Lambda architecture locked in at Task A,
consuming the Kafka topics in Task B built.

## Structure
```
flink/
  kafka_config.py               Shared topic/bootstrap config (mirrors Task_B's)
  incident_detection_job.py     Problem 9: AQI emergency, gridlock, bus bunching -> urbanpulse.incidents
jars/
  README.md                     Where to get the Flink Kafka connector JAR (not on pip)
spark/
  ward_energy_streaming.py      Problem 10: 15-min tumbling window, dual output (Kafka + Parquet)
  health_advisory_streaming_sql.py  Problem 11: rolling AQI avg + zone_profile join, Streaming SQL
docs/
  flink_vs_spark_comparison.md  Problem 12: 1-page Flink vs Spark analysis
```

## Why PyFlink + PySpark (not Java Flink)

Task B's `eta_enrichment.py` already established the pattern of using
Python end-to-end and documenting the "real" JVM API in the docstring
where no Python equivalent exists (Kafka Streams has no Python client).
Flink is different: **PyFlink is a real, first-class API**, not an
emulation — so unlike Kafka Streams, there's no forced trade-off here.
Staying Python keeps the whole submission (Task B + Task C) in one
language for the report and demo video, and PySpark is Spark's mature,
fully-supported Python API regardless.

## Running it end to end

Assumes Task B's Kafka cluster is already up (`docker compose up -d` from
`Task_B/`, topics created via `Task_B/config/create_topics.sh`).

```bash
# 1. Install Python deps
pip install apache-flink pyspark

# 2. Get the Flink Kafka connector JAR (see jars/README.md) and place it in jars/

# 3. Run the Flink incident detection job (problem 9)
#    Run Task B's producers in another terminal first/concurrently so there's
#    live data: python3 producers/bus_gps_producer.py, air_quality_producer.py,
#    traffic_signals_producer.py
cd flink
python3 incident_detection_job.py
#  -> watch urbanpulse.incidents in Kafka UI (localhost:8080) or via a console consumer

# 4. Run the Spark ward energy job (problem 10) -- needs Task B's
#    smart_meters_producer.py to have produced data first
cd ../spark
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2 \
    ward_energy_streaming.py
#  -> urbanpulse.ward_energy_summary topic + ./output/ward_energy_summary_parquet/

# 5. Run the Spark health advisory Streaming SQL job (problem 11) -- needs
#    Task B's air_quality_producer.py to have produced data first
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1 \
    health_advisory_streaming_sql.py
#  -> urbanpulse.health_advisories topic
```

## Design decisions (full justification also inline in each script's docstring)

| Job | Key mechanism | Justification |
|---|---|---|
| AQI Emergency | Keyed state (`sensor_id`) + alert cooldown | Emits on breach, suppresses re-alert spam during sustained hazardous readings |
| Traffic Gridlock | Keyed state (`junction_id`) tracking consecutive breach count | "3 consecutive cycles" requires a streak counter, not a window aggregate |
| Bus Bunching | Keyed state (`route_id`) with `MapState` of positions + pair-proximity timers | Continuous 5-min proximity between a *specific pair* of buses needs per-pair state, not a global aggregate |
| Ward Energy | 15-min tumbling window, 45-min watermark, dual sink | Long watermark justified by smart_meters' audit-not-alerting use case (Task B's 365-day retention) |
| Health Advisory | 10-min/1-min sliding window ("rolling avg"), 5-min watermark, Update mode | Short watermark justified by air_quality also feeding Flink's sub-2-min alerting |


## Notes for the report
- All event-time timestamps come from each record's own `timestamp` field
  (Task B's schema), never Kafka ingestion/processing time — required for
  the "3 consecutive cycles" / "5 continuous minutes" / "rolling average"
  semantics to be meaningful under out-of-order delivery.
- `zone_profile.csv` and `route_schedule.csv` (Task B's `data/` folder) are
  reused as-is for the problem 11 join and are referenced by relative path
  from `spark/health_advisory_streaming_sql.py` — no duplicated reference
  data.
- Aggregation logic for both Spark jobs was unit-tested against static
  DataFrames before wiring to Kafka (see conversation/test scripts) to
  confirm window boundaries and the join are correct independent of any
  Kafka connectivity issues.
