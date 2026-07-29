
# UrbanPulse - Smart City Streaming Platform

**DSE ZG556 (Stream Processing and Analytics) - Situated Learning Assignment - Group 34**

UrbanPulse is a **Lambda-architecture** data platform for a metropolitan transport and
environment authority, ingesting live bus GPS, traffic signal, air quality, and smart
meter data through Kafka, processing it through Flink (speed layer) and Spark (batch
layer), and serving it through dashboards, a health advisory API, and a signal-control
interface.

This repo contains the full implementation for **Task A** (architecture design),
**Task B** (Kafka ingestion layer), and **Task C** (Flink + Spark processing layer).

---

## Repository Structure

```
.
├── Task_B/
│   ├── config/
│   │   ├── kafka_config.py        # shared topic names, bootstrap servers, consumer groups
│   │   └── create_topics.sh       # creates all 5 topics with partitions/retention
│   ├── producers/
│   │   ├── bus_gps_producer.py
│   │   ├── traffic_signals_producer.py
│   │   ├── air_quality_producer.py
│   │   └── smart_meters_producer.py
│   ├── consumers/
│   │   └── priority_consumer_demo.py   # HIGH_PRIORITY vs STANDARD_PRIORITY consumer groups
│   ├── streams/
│   │   └── eta_enrichment.py      # GPS stream ⋈ static route-schedule table
│   ├── dlq/
│   │   ├── validators.py          # per-stream validation rules
│   │   └── dlq_report.py          # 5-minute DLQ error-type report
│   ├── data/                      # sample JSONL data + static CSVs (route_schedule, zone_profile)
│   ├── docker-compose.yml         # 3-broker KRaft Kafka cluster + Kafka UI
│   ├── requirements.txt
│   └── README.md                  # Task B-specific setup steps
├── Task_C/
│   ├── flink/
│   │   ├── incident_detection_job.py   # AQI emergency, traffic gridlock, bus bunching detectors
│   │   └── kafka_config.py
│   ├── spark/
│   │   ├── ward_energy_streaming.py         # 15-min tumbling window, dual sink (Kafka + Parquet)
│   │   └── health_advisory_streaming_sql.py # 10-min rolling AQI average, zone join
│   ├── jars/                      # Flink Kafka connector jar
│   └── README.md                  # Task C-specific setup steps
|
└── SPA_Assignment_UrbanPulse_Report_Group34.pdf
```


## Architecture Overview

**Ingestion Layer** -  Kafka (3-broker KRaft cluster, RF=3, min.insync.replicas=2)
**Speed Layer** - Apache Flink (PyFlink), event-time processing, keyed state, sub-2-minute incident detection
**Batch Layer** - Apache Spark Structured Streaming, windowed aggregation, dual sink (Kafka + Parquet)
**Storage Layer** - Apache Druid (time-series), PostgreSQL/PostGIS (spatial), MinIO/HDFS (archive/replay), PostgreSQL OLAP (compliance reporting)
**Serving Layer** - Superset dashboards, health advisory API, signal-control interface

Full design rationale, the Lambda-vs-Kappa evaluation matrix, and the government
readiness checklist are in the report: `SPA_Assignment_UrbanPulse_Report_Group34.pdf`.

---

## Prerequisites

- Docker & Docker Compose
- Python 3.10+
- Apache Flink 1.20.2 (PyFlink)
- Apache Spark 3.5.x
- Java 11+ (required by Flink/Spark)

---

## Quick Start

### 1. Start the Kafka cluster
```bash
cd Task_B
docker compose up -d
docker ps          # confirm kafka-1, kafka-2, kafka-3, kafka-ui are all up
```

### 2. Create topics
```bash
bash config/create_topics.sh
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --list
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Run producers
```bash
python producers/bus_gps_producer.py
python producers/traffic_signals_producer.py
python producers/air_quality_producer.py
python producers/smart_meters_producer.py
```

### 5. Run the priority consumer demo
```bash
python consumers/priority_consumer_demo.py
```

### 6. Run the ETA enrichment stream-table join
```bash
python streams/eta_enrichment.py
```

### 7. Generate the DLQ report
```bash
python dlq/dlq_report.py
```

### 8. Run the Flink incident detection job
```bash
cd ../Task_C/flink
python incident_detection_job.py
```

### 9. Run the Spark jobs
```bash
cd ../spark
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2 ward_energy_streaming.py
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2 health_advisory_streaming_sql.py
```

Full step-by-step instructions with expected output are in `Task_B/README.md` and
`Task_C/README.md`.

---

## Kafka Topics

| Topic | Retention | Partitions | Purpose |
|---|---|---|---|
| `urbanpulse.bus_gps` | Live bus position replay |
| `urbanpulse.traffic_signals` | Signal state, split across priority consumer groups |
| `urbanpulse.air_quality` | Pollution trend analysis |
| `urbanpulse.smart_meters` | Regulatory energy audit trail |
| `urbanpulse.dlq` | Dead-letter queue for all validation failures |
| `urbanpulse.incidents` | Unified Flink alert output (AQI/gridlock/bunching) |
| `urbanpulse.ward_energy_summary` | Spark ward-level 15-min aggregates |
| `urbanpulse.health_advisories` | Spark zone-level AQI advisories |

---

## Key Design Decisions

- **Lambda over Kappa**: driven by the 365-day smart-meter audit/reprocessing requirement - see report point 2.2 for the full evaluation matrix.
- **Separate consumer groups for priority isolation**: `HIGH_PRIORITY` and `STANDARD_PRIORITY` track independent offsets on the same topic, so a slow analytics consumer can never block real-time signal control.
- **Event-time processing in Flink**: all three detectors use watermarks, not processing time, since sensor/GPS data can arrive out of order.
- **Dual sink in Spark**: `ward_energy_streaming.py` writes simultaneously to Kafka (dashboards) and partitioned Parquet (audit archive).

---

## Documentation

- Full report (Task A, B, C): `SPA_Assignment_UrbanPulse_Report_Group34.pdf`
- Video walkthrough: `https://drive.google.com/file/d/14o2tp5TqO92mowTKPTfcdeBLsZD_qp5D/view?pli=1`

---

## Author

Group 34
```