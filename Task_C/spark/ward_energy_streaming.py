"""
UrbanPulse - Spark Structured Streaming: ward energy aggregation (Task C,problem 10)

Batch/serving-layer job. Consumes urbanpulse.smart_meters and computes, per
ward_id, per 15-minute TUMBLING window:
    - total_kwh_consumed  (sum of kwh_reading)
    - avg_power_factor    (avg of power_factor)
    - peak_voltage        (max of voltage)

Dual output (as required):
    1. ward_energy_summary Kafka topic  -- feeds councillor/ops dashboards
       needing near-live ward figures.
    2. Partitioned Parquet, partitioned by ward_id and date -- feeds the
       365-day regulatory energy audit use case that justified
       smart_meters' long retention in Task B, plus historical trend
       analysis for councillor reports (Task A pain point framing).

Late data handling
-------------------
smart_meters events carry their own `timestamp` field; we use THAT as event
time (not Kafka ingestion time), and apply a 45-MINUTE watermark. Smart
meters are the least real-time-critical of the four streams (Task B gave
them the longest retention, 365 days, precisely because they serve
audit/trend use cases rather than sub-2-minute alerting) -- so we can
afford to wait generously for stragglers (meter uploads over congested
ward networks, batched meter reads) rather than dropping billing-relevant
kWh. 45 min is 3x the window size, a common rule-of-thumb starting point,
and is explicitly justified in the report against this stream's SLA (none)
versus bus_gps/air_quality/traffic_signals, which feed the Flink speed
layer instead.

Running:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2 \
        ward_energy_streaming.py
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, sum as _sum, avg as _avg, max as _max, to_date,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
)

BOOTSTRAP_SERVERS = "localhost:9092,localhost:9094,localhost:9096"
TOPIC_SMART_METERS = "urbanpulse.smart_meters"
TOPIC_WARD_ENERGY_SUMMARY = "urbanpulse.ward_energy_summary"

OUTPUT_ROOT = os.environ.get("URBANPULSE_OUTPUT_ROOT", "./output")
PARQUET_PATH = f"{OUTPUT_ROOT}/ward_energy_summary_parquet"
CHECKPOINT_KAFKA = f"{OUTPUT_ROOT}/checkpoints/ward_energy_kafka_sink"
CHECKPOINT_PARQUET = f"{OUTPUT_ROOT}/checkpoints/ward_energy_parquet_sink"

SMART_METERS_SCHEMA = StructType([
    StructField("meter_id", StringType()),
    StructField("ward_id", StringType()),
    StructField("kwh_reading", DoubleType()),
    StructField("voltage", DoubleType()),
    StructField("power_factor", DoubleType()),
    StructField("timestamp", StringType()),  # ISO-8601; cast to timestamp below
])

WINDOW_DURATION = "15 minutes"
WATERMARK_DELAY = "45 minutes"  # justified above: smart_meters has no real-time SLA


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("urbanpulse-ward-energy-streaming")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1",
        )
        .getOrCreate()
    )


def read_smart_meters_stream(spark: SparkSession):
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_SMART_METERS)
        .option("startingOffsets", "earliest")
        .load()
    )
    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), SMART_METERS_SCHEMA).alias("rec"))
        .select("rec.*")
        .withColumn("event_time", col("timestamp").cast("timestamp"))
    )
    return parsed


def aggregate_ward_energy(parsed_stream):
    return (
        parsed_stream
        .withWatermark("event_time", WATERMARK_DELAY)
        .groupBy(
            col("ward_id"),
            window(col("event_time"), WINDOW_DURATION),
        )
        .agg(
            _sum("kwh_reading").alias("total_kwh_consumed"),
            _avg("power_factor").alias("avg_power_factor"),
            _max("voltage").alias("peak_voltage"),
        )
        .select(
            col("ward_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("total_kwh_consumed"),
            col("avg_power_factor"),
            col("peak_voltage"),
        )
    )


def write_to_kafka(ward_summary_df):
    kafka_payload = (
        ward_summary_df
        .selectExpr(
            "ward_id AS key",
            "to_json(struct(*)) AS value",
        )
    )
    return (
        kafka_payload.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("topic", TOPIC_WARD_ENERGY_SUMMARY)
        .option("checkpointLocation", CHECKPOINT_KAFKA)
        # append, not update: with a watermark set, append mode emits each
        # window row exactly once, when the watermark passes it (i.e. the
        # window is "closed") -- this avoids pushing partial/still-changing
        # 15-min totals to the councillor dashboard before the window is
        # actually final.
        .outputMode("append")
        .start()
    )


def write_to_parquet(ward_summary_df):
    partitioned = ward_summary_df.withColumn("date", to_date(col("window_start")))
    return (
        partitioned.writeStream
        .format("parquet")
        .option("path", PARQUET_PATH)
        .option("checkpointLocation", CHECKPOINT_PARQUET)
        .partitionBy("ward_id", "date")
        .outputMode("append")  # Parquet sink requires append; rows land once
        .start()               # their window closes (watermark advances past it)
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    parsed = read_smart_meters_stream(spark)
    ward_summary = aggregate_ward_energy(parsed)

    kafka_query = write_to_kafka(ward_summary)
    parquet_query = write_to_parquet(ward_summary)

    print(f"Streaming ward energy summary -> Kafka topic '{TOPIC_WARD_ENERGY_SUMMARY}' "
          f"and Parquet at '{PARQUET_PATH}' (partitioned by ward_id, date)")

    kafka_query.awaitTermination()
    parquet_query.awaitTermination()
    #spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
