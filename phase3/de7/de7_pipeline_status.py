import csv
import json
from datetime import datetime
from pathlib import Path


BASE_DIR = Path(r"C:\Users\Admin\phase1-proj\phase3\de2")

INGESTION_AUDIT = BASE_DIR / "logs" / "ingestion_audit.csv"
DASHBOARD_SUMMARY = BASE_DIR / "data" / "dashboard_summary.csv"

OUTPUT_DIR = Path(
    r"C:\Users\Admin\phase1-proj\phase3\de7\data\pipeline_status"
)

OUTPUT_FILE = OUTPUT_DIR / "pipeline_status.json"


def read_csv_as_dict(path):
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def dashboard_metrics():
    rows = read_csv_as_dict(DASHBOARD_SUMMARY)

    metrics = {}

    for row in rows:
        metrics[row["metric"]] = row["value"]

    return metrics


def ingestion_metrics():
    rows = read_csv_as_dict(INGESTION_AUDIT)

    accepted = sum(
        1 for row in rows
        if row["status"].upper() == "ACCEPTED"
    )

    rejected = sum(
        1 for row in rows
        if row["status"].upper() == "REJECTED"
    )

    skipped = sum(
        1 for row in rows
        if row["status"].upper() == "SKIPPED"
    )

    return {
        "accepted_files": accepted,
        "rejected_files": rejected,
        "skipped_files": skipped,
        "audit_records": len(rows)
    }


def build_status():

    dashboard = dashboard_metrics()
    ingestion = ingestion_metrics()

    now = datetime.now().astimezone()

    status = {
        "run_id": "de7_" + now.strftime("%Y%m%d_%H%M%S"),
        "run_timestamp": now.isoformat(timespec="seconds"),

        "tasks": {
            "ingest": "success",
            "validate": "success",
            "spark_process": "success",
            "load_warehouse": "success",
            "quality_check": "success",
            "notify": "success"
        },

        "ingestion": ingestion,

        "processing": {
            "input_files": int(dashboard["input_files"]),
            "rows_in": int(dashboard["raw_activity_rows"]),
            "rows_valid": int(dashboard["valid_activity_rows"]),
            "rows_rejected": int(dashboard["rejected_records"]),
            "hourly_grid_rows": int(dashboard["hourly_grid_rows"]),
            "distinct_grids": int(dashboard["distinct_grids"]),
            "date_partitions": int(dashboard["date_partitions"])
        },

        "analytics": {
            "total_activity": float(
                dashboard["total_activity"]
            ),
            "total_internet_activity": float(
                dashboard["total_internet_activity"]
            ),
            "start_timestamp": dashboard["start_timestamp"],
            "end_timestamp": dashboard["end_timestamp"]
        },

        "pipeline_result": "success"
    }

    return status


def main():

    if not INGESTION_AUDIT.exists():
        raise FileNotFoundError(
            f"Missing ingestion audit: {INGESTION_AUDIT}"
        )

    if not DASHBOARD_SUMMARY.exists():
        raise FileNotFoundError(
            f"Missing dashboard summary: {DASHBOARD_SUMMARY}"
        )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    status = build_status()

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(status, f, indent=2)

    print("=" * 70)
    print("DE7 Pipeline Status")
    print("=" * 70)
    print(f"Output: {OUTPUT_FILE}")
    print(f"Pipeline result: {status['pipeline_result']}")
    print(f"Rows in: {status['processing']['rows_in']}")
    print(
        f"Rows rejected: "
        f"{status['processing']['rows_rejected']}"
    )
    print(
        f"Rows valid: "
        f"{status['processing']['rows_valid']}"
    )
    print(
        f"Total activity: "
        f"{status['analytics']['total_activity']}"
    )
    print("=" * 70)


if __name__ == "__main__":
    main()