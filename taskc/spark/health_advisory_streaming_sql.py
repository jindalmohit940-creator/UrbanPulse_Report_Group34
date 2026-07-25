"""
UrbanPulse - Streaming SQL health advisories (Task C, problem 11)


Pipeline:
  (a) 10-minute ROLLING average AQI per zone.
      Structured Streaming has no native per-event rolling-average operator,
      so this is implemented as a SLIDING window: window(event_time,
      "10 minutes", "1 minute") -- a 10-minute window that re-triggers every
      1 minute. That re-triggering is exactly what makes it "rolling" rather
      than a fixed tumbling bucket, and it's why Update output mode (not
      Append) is required: each 1-minute tick can revise an in-flight
      10-minute window's average as new AQI readings land in it.
  (b) Joins the rolling average with the static zone_profile reference table
      (zone, zone_name, population, num_schools -- same file Task B ships
      at data/zone_profile.csv) to produce a human-readable, risk-weighted
      advisory: more schools/higher population in a zone raises the
      real-world stakes of the same AQI number.
  (c) Filters to rolling_avg_aqi > 150 (Unhealthy, per the standard AQI
      breakpoint table) and writes to urbanpulse.health_advisories in
      Update output mode.

Running:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2 \
        health_advisory_streaming_sql.py
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

BOOTSTRAP_SERVERS = "localhost:9092,localhost:9094,localhost:9096"
TOPIC_AIR_QUALITY = "urbanpulse.air_quality"
TOPIC_HEALTH_ADVISORIES = "urbanpulse.health_advisories"

ZONE_PROFILE_CSV = os.environ.get(
    "URBANPULSE_ZONE_PROFILE_CSV",
    os.path.join(os.path.dirname(__file__), "..", "..", "Task_B", "data", "zone_profile.csv"),
)
CHECKPOINT = os.environ.get(
    "URBANPULSE_OUTPUT_ROOT", "./output"
) + "/checkpoints/health_advisory_sink"

AIR_QUALITY_SCHEMA = StructType([
    StructField("sensor_id", StringType()),
    StructField("zone", StringType()),
    StructField("pm25", DoubleType()),
    StructField("pm10", DoubleType()),
    StructField("no2", DoubleType()),
    StructField("aqi", DoubleType()),
    StructField("timestamp", StringType()),
])

UNHEALTHY_AQI_THRESHOLD = 150
ROLLING_WINDOW = "10 minutes"
ROLLING_SLIDE = "1 minute"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("urbanpulse-health-advisory-streaming-sql")
       .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2",
    )
        .getOrCreate()
    )


def register_air_quality_stream_view(spark: SparkSession):
    """Reads urbanpulse.air_quality, parses it, and registers it as a
    streaming temp view so the actual aggregation/join/filter below can be
    expressed as real SQL."""
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_AIR_QUALITY)
        .option("startingOffsets", "earliest")
        .load()
    )
    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json_str")
        .select("json_str")
    )
    parsed.createOrReplaceTempView("air_quality_raw")

    # from_json needs the schema in scope for SQL -- register via a DF
    # transform, then re-expose as a view (SQL alone can't take a Python
    # schema object as a literal).
    from pyspark.sql.functions import from_json, col
    structured = (
        spark.table("air_quality_raw")
        .select(from_json(col("json_str"), AIR_QUALITY_SCHEMA).alias("rec"))
        .select("rec.*")
        .withColumn("event_time", col("timestamp").cast("timestamp"))
        # Bounds state size for the sliding-window aggregation below and
        # lets Spark eventually evict old window state. 5 min is short
        # deliberately -- air_quality also feeds Flink's sub-2-minute AQI
        # alerting (problem 9a), so this stream is expected to be prompt;
        # unlike smart_meters (45 min watermark), there's no billing/audit
        # reason to wait long for stragglers here.
        .withWatermark("event_time", "5 minutes")
    )
    structured.createOrReplaceTempView("air_quality_stream")


def register_zone_profile_view(spark: SparkSession):
    """Static reference table -- read once as a batch DataFrame and
    registered as a plain (non-streaming) temp view for the stream-static
    join in the SQL below."""
    zone_profile = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(ZONE_PROFILE_CSV)
    )
    zone_profile.createOrReplaceTempView("zone_profile")


def run_health_advisory_sql(spark: SparkSession):
    """The actual Streaming SQL query (problem 11 a+b+c) as one statement."""
    query_sql = f"""
        SELECT
            aq.zone,
            zp.zone_name,
            zp.population,
            zp.num_schools,
            window(aq.event_time, '{ROLLING_WINDOW}', '{ROLLING_SLIDE}').start AS window_start,
            window(aq.event_time, '{ROLLING_WINDOW}', '{ROLLING_SLIDE}').end   AS window_end,
            AVG(aq.aqi) AS rolling_avg_aqi
        FROM air_quality_stream aq
        JOIN zone_profile zp
            ON aq.zone = zp.zone
        GROUP BY
            aq.zone, zp.zone_name, zp.population, zp.num_schools,
            window(aq.event_time, '{ROLLING_WINDOW}', '{ROLLING_SLIDE}')
        HAVING AVG(aq.aqi) > {UNHEALTHY_AQI_THRESHOLD}
    """
    advisories = spark.sql(query_sql)
    return advisories


def write_to_kafka(advisories_df):
    kafka_payload = advisories_df.selectExpr(
        "zone AS key",
        "to_json(struct(*)) AS value",
    )
    return (
        kafka_payload.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("topic", TOPIC_HEALTH_ADVISORIES)
        .option("checkpointLocation", CHECKPOINT)
        .outputMode("update")  # required by the spec: sliding window can
        .start()                # revise an in-flight 10-min average every 1 min
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    register_air_quality_stream_view(spark)
    register_zone_profile_view(spark)
    advisories = run_health_advisory_sql(spark)

    query = write_to_kafka(advisories)
    print(f"Streaming health advisories (rolling_avg_aqi > {UNHEALTHY_AQI_THRESHOLD}) "
          f"-> Kafka topic '{TOPIC_HEALTH_ADVISORIES}'")
    query.awaitTermination()


if __name__ == "__main__":
    main()
