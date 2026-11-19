"""Validate Kafka appointment events and write Spark micro-batches to PostgreSQL."""

import json
import logging
import os
from datetime import datetime, timezone

LOGGER = logging.getLogger(__name__)
STATUSES = {"completed", "cancelled", "scheduled"}
MAX_LAB_TESTS = 2_147_483_647
INSERT_EVENTS = """
    INSERT INTO raw.ingested_events (
        topic, kafka_partition, kafka_offset, kafka_timestamp,
        appointment_id, department, status, lab_tests, rejection_reason
    ) VALUES %s
    ON CONFLICT (topic, kafka_partition, kafka_offset) DO NOTHING
"""


def required_text(event, field):
    value = event.get(field)
    if isinstance(value, str):
        value = value.strip()
        if value and len(value) <= 128 and "\x00" not in value:
            try:
                value.encode("utf-8")
            except UnicodeEncodeError:
                pass
            else:
                return value
    raise ValueError(f"invalid_{field}")


def validate(value):
    try:
        event = json.loads(value.decode("utf-8")) if value is not None else None
    except (UnicodeError, ValueError, RecursionError):
        raise ValueError("malformed_json") from None
    if not isinstance(event, dict):
        raise ValueError("invalid_event")

    appointment_id = required_text(event, "appointment_id")
    department = required_text(event, "department")
    status = event.get("status")
    if not isinstance(status, str) or status not in STATUSES:
        raise ValueError("invalid_status")

    tests = event.get("lab_tests")
    if isinstance(tests, str) and len(tests) <= 10 and tests.isascii() and tests.isdecimal():
        tests = int(tests)
    if type(tests) is not int or not 0 <= tests <= MAX_LAB_TESTS:
        raise ValueError("invalid_lab_tests")
    return appointment_id, department, status, tests


def classify(value):
    try:
        return (*validate(value), None)
    except ValueError as exc:
        return None, None, None, None, str(exc)


def write_batch(batch, batch_id):
    from contextlib import closing

    import psycopg2
    from psycopg2.extras import execute_values

    rows = [
        (
            row.topic,
            row.partition,
            row.offset,
            datetime.fromtimestamp(row.timestamp_ms / 1000, tz=timezone.utc),
            *row.result,
        )
        for row in batch.toLocalIterator()
    ]
    if not rows:
        return

    with closing(psycopg2.connect(os.environ["WAREHOUSE_DSN"], connect_timeout=10)) as connection:
        with connection, connection.cursor() as cursor:
            execute_values(cursor, INSERT_EVENTS, rows)

    rejected = sum(row[-1] is not None for row in rows)
    LOGGER.info("Batch %s committed with %s valid and %s rejected events", batch_id, len(rows) - rejected, rejected)


def main():
    from pyspark.sql import SparkSession, functions as F, types as T

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = SparkSession.builder.appName("healthcare-appointments").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    result_type = T.StructType([
        T.StructField("appointment_id", T.StringType()),
        T.StructField("department", T.StringType()),
        T.StructField("status", T.StringType()),
        T.StructField("lab_tests", T.IntegerType()),
        T.StructField("rejection_reason", T.StringType()),
    ])
    classify_event = F.udf(classify, result_type)
    events = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.environ["KAFKA_BOOTSTRAP_SERVERS"])
        .option("subscribe", os.environ["KAFKA_TOPIC"])
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 1000)
        .load()
        .select(
            "topic", "partition", "offset",
            F.unix_millis("timestamp").alias("timestamp_ms"),
            classify_event("value").alias("result"),
        )
    )
    query = (
        events.writeStream
        .option("checkpointLocation", os.environ["SPARK_CHECKPOINT"])
        .trigger(processingTime="5 seconds")
        .foreachBatch(write_batch)
        .start()
    )
    try:
        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
