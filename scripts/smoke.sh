#!/usr/bin/env bash
set -euo pipefail

wait_for() {
    for attempt in {1..60}; do
        if "$@"; then
            return 0
        fi
        sleep 5
    done
    printf 'Timed out waiting for %s\n' "$*" >&2
    return 1
}

publish() {
    docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
        --bootstrap-server kafka:9092 --topic healthcare.appointments \
        --property parse.key=true --producer-property acks=all
}

warehouse_query() {
    docker compose exec -T postgres psql -X -U postgres -d healthcare \
        -At -v ON_ERROR_STOP=1 -c "$1"
}

ingestion_complete() {
    local result
    result=$(warehouse_query "
        SELECT (SELECT count(*) FROM raw.appointments) = 4
           AND (SELECT count(*) FROM raw.rejected_events WHERE rejection_reason = 'invalid_status') = 1
           AND (SELECT count(*) FROM raw.rejected_events WHERE rejection_reason = 'malformed_json') = 1
    ")
    [[ "$result" == t ]]
}

airflow_url=http://127.0.0.1:8080/api/v1/dags/healthcare_analytics
grafana_url=http://127.0.0.1:3000
run_id="smoke-$(date +%s)"

airflow_ready() {
    curl --fail --silent --max-time 10 -u admin:admin "$airflow_url" >/dev/null
}

dag_succeeded() {
    curl --fail --silent --max-time 10 -u admin:admin "$airflow_url/dagRuns/$run_id" |
        python3 -c 'import json, sys; sys.exit(json.load(sys.stdin)["state"] != "success")' 2>/dev/null
}

grafana_ready() {
    curl --fail --silent --max-time 10 -u admin:admin \
        "$grafana_url/api/dashboards/uid/healthcare-operations" >/dev/null
}

# Send the four fixture rows, a duplicate valid appointment, and malformed JSON.
{
    python3 scripts/publish_sample.py
    printf '%s\t%s\n' A1 '{"appointment_id":"A1","department":"Cardiology","status":"completed","lab_tests":2}'
    printf '%s\t%s\n' bad '{'
} | publish
wait_for ingestion_complete

wait_for airflow_ready
curl --fail --silent --show-error --max-time 15 -u admin:admin \
    -H 'Content-Type: application/json' -d "{\"dag_run_id\":\"$run_id\"}" \
    "$airflow_url/dagRuns" >/dev/null
wait_for dag_succeeded

wait_for grafana_ready
curl --fail --silent --show-error --max-time 20 -u admin:admin \
    -H 'Content-Type: application/json' -d @- "$grafana_url/api/ds/query" <<'JSON' |
{
  "from": "now-6h",
  "to": "now",
  "queries": [
    {
      "refId": "A",
      "datasource": {"type": "grafana-postgresql-datasource", "uid": "healthcare-postgres"},
      "format": "table",
      "rawQuery": true,
      "rawSql": "SELECT SUM(appointments)::int AS appointments, SUM(lab_tests)::int AS lab_tests, (SELECT SUM(rejected_events)::int FROM analytics.rejection_metrics) AS rejected FROM analytics.department_metrics"
    }
  ]
}
JSON
    python3 -c 'import json, sys; result = json.load(sys.stdin)["results"]["A"]; assert result["frames"][0]["data"]["values"] == [[3], [3], [2]], result'

printf 'Pipeline verified with 3 appointments, 3 lab tests, and 2 rejected events.\n'
