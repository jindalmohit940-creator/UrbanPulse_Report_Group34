"""Shared config for all UrbanPulse Kafka producers/consumers."""

BOOTSTRAP_SERVERS = ["localhost:9092", "localhost:9094", "localhost:9096"]

TOPIC_BUS_GPS = "urbanpulse.bus_gps"
TOPIC_TRAFFIC_SIGNALS = "urbanpulse.traffic_signals"
TOPIC_AIR_QUALITY = "urbanpulse.air_quality"
TOPIC_SMART_METERS = "urbanpulse.smart_meters"
TOPIC_DLQ = "urbanpulse.dlq"

CONSUMER_GROUP_HIGH_PRIORITY = "traffic-signals-high-priority"
CONSUMER_GROUP_STANDARD_PRIORITY = "traffic-signals-standard-priority"
