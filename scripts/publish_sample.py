"""Convert the synthetic fixture to keyed JSON records for kafka-console-producer."""

import csv
import json
from pathlib import Path


def main():
    sample = Path(__file__).resolve().parents[1] / "data" / "input.csv"
    with sample.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            event = {field: row[field] for field in ("appointment_id", "department", "status", "lab_tests")}
            print(f"{event['appointment_id']}\t{json.dumps(event)}")


if __name__ == "__main__":
    main()
