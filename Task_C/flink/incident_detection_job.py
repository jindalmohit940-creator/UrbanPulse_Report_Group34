"""
UrbanPulse - Flink real-time incident detection (Task C, problem 9)

Speed-layer job. Consumes three Task B topics and detects three incident
types using KEYED STATE + EVENT-TIME WATERMARKS, emitting all alerts to a
single urbanpulse.incidents Kafka topic:

  (a) AQI Emergency   -- air_quality: aqi > 300 (Hazardous)
                          keyed by sensor_id. Target: alert within 2 min of
                          the reading (Task A pain point #3).
  (b) Traffic Gridlock -- traffic_signals:  avg_wait_sec > 180 for 3
                          CONSECUTIVE signal cycles on the same junction.
                          keyed by junction_id. Target: adaptive response
                          within 90s of congestion detection (pain point #1).
  (c) Bus Bunching    -- bus_gps: two buses on the same route_id
                          within 200m of each other for > 5 continuous
                          minutes. keyed by route_id. Feeds the ETA
                          reliability story from Task B's eta_enrichment.

Design notes
------------
- Event time is taken from each record's own `timestamp` field (ISO-8601,
  matching Task B's schema), NOT processing time -- this is what makes the
  "3 consecutive cycles" / "5 continuous minutes" conditions meaningful even
  if Kafka delivery is briefly out of order or delayed.
- BoundedOutOfOrdernessWatermarks with a 30s bound: sensors/GPS trackers can
  arrive slightly out of order (network jitter across 12,000 buses / 3,800
  signals), but UrbanPulse's sub-2-minute alert SLA means we cannot afford a
  large watermark delay -- 30s is a deliberate trade-off between correctness
  and latency, documented for the report.
- Each detector is a KeyedProcessFunction so incident state (consecutive
  breach counts, per-bus-pair proximity windows, per-sensor alert cooldowns)
  is scoped per key and fault-tolerant via Flink's state backend --
  this is the actual "keyed state" the assignment asks for, not just a
  windowed aggregation.
- All three alert streams are unioned into one DataStream[str] (JSON) and
  written to urbanpulse.incidents via a single KafkaSink, so downstream
  consumers (dashboards, the ops centre) only need to watch one topic.

Running (Task_C/README.md for full setup):
    python3 flink/incident_detection_job.py
Requires flink-sql-connector-kafka-<version>.jar on the classpath -- same Docker-Kafka-cluster-on-Windows pattern as
Task B, just Flink talking to the same brokers instead of a Python client.
"""
import json
import math
import os
from pathlib import Path
from datetime import datetime, timezone

from pyflink.common import Types, WatermarkStrategy, Duration, Time
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, RuntimeContext
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor, MapStateDescriptor
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema,
)

from kafka_config import (
    BOOTSTRAP_SERVERS, TOPIC_BUS_GPS, TOPIC_TRAFFIC_SIGNALS,
    TOPIC_AIR_QUALITY, TOPIC_INCIDENTS,
)

JARS_DIR = os.path.join(os.path.dirname(__file__), "..", "jars")
AQI_HAZARDOUS_THRESHOLD = 300
GRIDLOCK_WAIT_THRESHOLD_SEC = 180
GRIDLOCK_CONSECUTIVE_CYCLES = 3
BUNCHING_DISTANCE_M = 200
BUNCHING_DURATION_MS = 5 * 60 * 1000
AQI_ALERT_COOLDOWN_MS = 2 * 60 * 1000  # avoid re-alerting every reading while still hazardous
WATERMARK_BOUND = Duration.of_seconds(30)


def _to_epoch_millis(iso_ts: str) -> int:
    """Parses the schema's ISO-8601 `timestamp` field to epoch millis for
    event-time assignment. Handles trailing 'Z' (UTC) as Task B's data does."""
    ts = iso_ts.replace("Z", "+00:00")
    return int(datetime.fromisoformat(ts).timestamp() * 1000)


def _haversine_m(lat1, lon1, lat2, lon2) -> float:
    """Great-circle distance in metres -- used for the 200m bus-bunching check."""
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlambda / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


class AqiEmergencyDetector(KeyedProcessFunction):
    """Keyed by sensor_id. Emits an alert the moment a reading exceeds the
    Hazardous threshold, then suppresses repeat alerts for the same sensor
    for AQI_ALERT_COOLDOWN_MS so a sustained hazardous zone doesn't spam the
    incidents topic once per reading."""

    def open(self, ctx: RuntimeContext):
        self.last_alert_ts = ctx.get_state(
            ValueStateDescriptor("last_aqi_alert_ts", Types.LONG())
        )

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        rec = json.loads(value)
        aqi = rec.get("aqi")
        if aqi is None or aqi <= AQI_HAZARDOUS_THRESHOLD:
            return
        event_ts = ctx.timestamp()
        last = self.last_alert_ts.value()
        if last is not None and event_ts - last < AQI_ALERT_COOLDOWN_MS:
            return
        self.last_alert_ts.update(event_ts)
        yield json.dumps({
            "incident_type": "AQI_EMERGENCY",
            "severity": "HAZARDOUS",
            "sensor_id": rec.get("sensor_id"),
            "zone": rec.get("zone"),
            "aqi": aqi,
            "source_timestamp": rec.get("timestamp"),
            "detected_at_event_time": event_ts,
        })


class TrafficGridlockDetector(KeyedProcessFunction):
    """Keyed by junction_id. Tracks CONSECUTIVE signal cycles where
    avg_wait_sec > 180 using keyed ValueState. Any cycle at/under threshold
    resets the streak -- this is what makes it "3 consecutive", not just
    "3 out of the last N"."""

    def open(self, ctx: RuntimeContext):
        self.consecutive_breaches = ctx.get_state(
            ValueStateDescriptor("consecutive_gridlock_cycles", Types.INT())
        )
        self.alerted_for_streak = ctx.get_state(
            ValueStateDescriptor("alerted_for_current_streak", Types.BOOLEAN())
        )

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        rec = json.loads(value)
        wait = rec.get("avg_wait_sec")
        if wait is None:
            return
        event_ts = ctx.timestamp()

        if wait > GRIDLOCK_WAIT_THRESHOLD_SEC:
            count = (self.consecutive_breaches.value() or 0) + 1
            self.consecutive_breaches.update(count)
            already_alerted = self.alerted_for_streak.value() or False
            if count >= GRIDLOCK_CONSECUTIVE_CYCLES and not already_alerted:
                self.alerted_for_streak.update(True)
                yield json.dumps({
                    "incident_type": "TRAFFIC_GRIDLOCK",
                    "severity": "HIGH",
                    "junction_id": rec.get("junction_id"),
                    "zone": rec.get("zone"),
                    "consecutive_cycles_over_threshold": count,
                    "avg_wait_sec": wait,
                    "source_timestamp": rec.get("timestamp"),
                    "detected_at_event_time": event_ts,
                })
        else:
            self.consecutive_breaches.update(0)
            self.alerted_for_streak.update(False)


class BusBunchingDetector(KeyedProcessFunction):
    """Keyed by route_id. Maintains a MapState of the latest known position
    per bus_id on the route, and a second MapState tracking how long each
    bus PAIR has been continuously within BUNCHING_DISTANCE_M. When a pair's
    continuous-proximity duration exceeds 5 minutes, emits one alert (then
    suppresses further alerts for that pair until they separate)."""

    def open(self, ctx: RuntimeContext):
        self.positions = ctx.get_map_state(
            MapStateDescriptor("bus_positions", Types.STRING(), Types.PICKLED_BYTE_ARRAY())
        )
        # pair_key -> (first_close_event_ts, already_alerted)
        self.pair_proximity = ctx.get_map_state(
            MapStateDescriptor("pair_proximity", Types.STRING(), Types.PICKLED_BYTE_ARRAY())
        )

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        rec = json.loads(value)
        bus_id = rec.get("bus_id")
        lat, lon = rec.get("lat"), rec.get("lon")
        if bus_id is None or lat is None or lon is None:
            return
        #event_ts = ctx.timestamp()
        #print(f"[BUNCHING] bus={bus_id} ts={event_ts} known_positions={list(self.positions.keys())}", flush=True)
        event_ts = _to_epoch_millis(rec["timestamp"])
        #print(f"[BUNCHING] bus={bus_id} ts={event_ts} known_positions={list(self.positions.keys())}", flush=True)

        for other_bus_id in list(self.positions.keys()):
            if other_bus_id == bus_id:
                continue
            other_lat, other_lon, other_ts = self.positions.get(other_bus_id)
            # Only compare against reasonably fresh positions of the other bus
            # (stale GPS shouldn't count as "currently bunched").
            if event_ts - other_ts > 3 * 60 * 1000:
                continue

            dist = _haversine_m(lat, lon, other_lat, other_lon)
            pair_key = "|".join(sorted([bus_id, other_bus_id]))

            if dist <= BUNCHING_DISTANCE_M:
                existing = self.pair_proximity.get(pair_key)
                #print(f"[BUNCHING] pair={pair_key} dist={dist:.1f}m existing_state={existing}", flush=True)
                if existing is None:
                    self.pair_proximity.put(pair_key, (event_ts, False))
                else:
                    first_ts, already_alerted = existing
                    duration = event_ts - first_ts
                    if duration >= BUNCHING_DURATION_MS and not already_alerted:
                        self.pair_proximity.put(pair_key, (first_ts, True))
                        route_id = rec.get("route_id")
                        yield json.dumps({
                            "incident_type": "BUS_BUNCHING",
                            "severity": "MEDIUM",
                            "route_id": route_id,
                            "bus_id_1": bus_id,
                            "bus_id_2": other_bus_id,
                            "distance_m": round(dist, 1),
                            "continuous_proximity_sec": duration // 1000,
                            "source_timestamp": rec.get("timestamp"),
                            "detected_at_event_time": event_ts,
                        })
            else:
                # buses separated -- clear the streak so a future re-approach
                # starts a fresh 5-minute clock instead of reusing old state.
                if self.pair_proximity.contains(pair_key):
                    self.pair_proximity.remove(pair_key)

        self.positions.put(bus_id, (lat, lon, event_ts))


def build_source(topic: str, group_id: str) -> KafkaSource:
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_topics(topic)
        .set_group_id(group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def extract_event_time(value: str) -> int:
    rec = json.loads(value)
    return _to_epoch_millis(rec["timestamp"])

class _EventTimeAssigner(TimestampAssigner):
        def extract_timestamp(self, value, record_timestamp):
            return extract_event_time(value)
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # single-node demo cluster; see README for scale-out notes
    #env.add_jars(*[
     #   f"file://{JARS_DIR}/{f}" for f in os.listdir(JARS_DIR) if f.endswith(".jar")
    #])
    jar_dir = Path(JARS_DIR).resolve()
    jar_uris = [jar_dir.joinpath(f).as_uri() for f in os.listdir(jar_dir) if f.endswith(".jar")]
    print(f"Loading Flink connector JARs: {jar_uris}")
    env.add_jars(*jar_uris)

    #watermark_strategy = (
    #    WatermarkStrategy
    #    .for_bounded_out_of_orderness(WATERMARK_BOUND)
    #    .with_timestamp_assigner(lambda value, ts: extract_event_time(value))
    #)
    

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(WATERMARK_BOUND)
        .with_timestamp_assigner(_EventTimeAssigner())
    )

    aqi_stream = env.from_source(
        build_source(TOPIC_AIR_QUALITY, "flink-incident-aqi"),
        watermark_strategy, "air_quality_source",
    )
    traffic_stream = env.from_source(
        build_source(TOPIC_TRAFFIC_SIGNALS, "flink-incident-traffic"),
        watermark_strategy, "traffic_signals_source",
    )
    bus_stream = env.from_source(
        build_source(TOPIC_BUS_GPS, "flink-incident-busgps"),
        watermark_strategy, "bus_gps_source",
    )
   # bus_stream = env.from_source(
    #    build_source("bus_gps_test", "flink-incident-busgps-test1"),
    #    watermark_strategy, "bus_gps_source",
    #)

    aqi_alerts = (
        aqi_stream
        .key_by(lambda v: json.loads(v).get("sensor_id", ""))
        .process(AqiEmergencyDetector(), output_type=Types.STRING())
    )
    gridlock_alerts = (
        traffic_stream
        .key_by(lambda v: json.loads(v).get("junction_id", ""))
        .process(TrafficGridlockDetector(), output_type=Types.STRING())
    )
    bunching_alerts = (
        bus_stream
        .key_by(lambda v: json.loads(v).get("route_id", ""))
        .process(BusBunchingDetector(), output_type=Types.STRING())
    )

    incidents = aqi_alerts.union(gridlock_alerts, bunching_alerts)

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(TOPIC_INCIDENTS)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )
    incidents.sink_to(sink)
    incidents.print()  # also echo to job log / console for the demo video

    env.execute("urbanpulse-incident-detection")


if __name__ == "__main__":
    main()
