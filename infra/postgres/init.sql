CREATE ROLE warehouse LOGIN PASSWORD 'warehouse';
CREATE ROLE grafana_reader LOGIN PASSWORD 'grafana_reader';
CREATE ROLE airflow LOGIN PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;

CREATE SCHEMA raw AUTHORIZATION warehouse;
CREATE SCHEMA analytics AUTHORIZATION warehouse;
GRANT USAGE ON SCHEMA analytics TO grafana_reader;

SET ROLE warehouse;

CREATE TABLE raw.ingested_events (
    topic text NOT NULL,
    kafka_partition integer NOT NULL,
    kafka_offset bigint NOT NULL,
    kafka_timestamp timestamptz NOT NULL,
    appointment_id text,
    department text,
    status text,
    lab_tests integer,
    rejection_reason text,
    PRIMARY KEY (topic, kafka_partition, kafka_offset),
    CHECK (
        (rejection_reason IS NULL AND appointment_id IS NOT NULL
         AND department IS NOT NULL AND status IS NOT NULL
         AND status IN ('completed', 'cancelled', 'scheduled')
         AND lab_tests IS NOT NULL AND lab_tests >= 0)
        OR
        (rejection_reason IS NOT NULL AND appointment_id IS NULL
         AND department IS NULL AND status IS NULL AND lab_tests IS NULL)
    )
);

CREATE VIEW raw.appointments AS
SELECT topic, kafka_partition, kafka_offset, kafka_timestamp, appointment_id, department, status, lab_tests
FROM raw.ingested_events
WHERE rejection_reason IS NULL;

CREATE VIEW raw.rejected_events AS
SELECT topic, kafka_partition, kafka_offset, kafka_timestamp, rejection_reason
FROM raw.ingested_events
WHERE rejection_reason IS NOT NULL;

RESET ROLE;
