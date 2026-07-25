"""Shared Kafka config for Task C Flink jobs. Mirrors Task_B/config/kafka_config.py
so bootstrap servers / topic names stay consistent across the whole pipeline."""

BOOTSTRAP_SERVERS = "localhost:9092,localhost:9094,localhost:9096"

TOPIC_BUS_GPS = "urbanpulse.bus_gps"
TOPIC_TRAFFIC_SIGNALS = "urbanpulse.traffic_signals"
TOPIC_AIR_QUALITY = "urbanpulse.air_quality"
TOPIC_INCIDENTS = "urbanpulse.incidents"
