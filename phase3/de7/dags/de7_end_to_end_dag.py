from datetime import datetime, timedelta
from pathlib import Path
import csv
import json
import os
import re
import sqlite3
import subprocess

from airflow import DAG
from airflow.models.taskinstance import TaskInstance
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule


# =========================================================
# DE7 PATHS
# =========================================================

PROJECT_ROOT_WIN = Path(
    r"C:\Users\semmozhiselvan.a\Documents\phase1 proj"
)

PROJECT_ROOT_WSL = Path(
    "/mnt/c/Users/semmozhiselvan.a/Documents/phase1 proj"
)

PROJECT_WIN = PROJECT_ROOT_WIN / "phase3"
PROJECT_WSL = PROJECT_ROOT_WSL / "phase3"

DE2_WIN = PROJECT_WIN / "de2"
DE2_WSL = PROJECT_WSL / "de2"

DE6_WIN = PROJECT_WIN / "de6"
DE6_WSL = PROJECT_WSL / "de6"

DE7_WIN = PROJECT_WIN / "de7"
DE7_WSL = PROJECT_WSL / "de7"

PHASE6_WIN = PROJECT_ROOT_WIN / "phase6"

PYTHON_EXE_WIN = r"C:\Program Files\Python311\python.exe"


# =========================================================
# WINDOWS SCRIPTS
# =========================================================

INGESTION_SCRIPT_WIN = str(
    DE2_WIN / "ingestion" / "ingestion.py"
)

SPARK_SCRIPT_WIN = str(
    DE2_WIN / "spark" / "telecom_pipeline.py"
)

WAREHOUSE_SCRIPT_WIN = str(
    DE6_WIN / "scripts" / "build_warehouse.py"
)

ML2_SCRIPT_WIN = str(
    PHASE6_WIN / "ml2" / "ml" / "features.py"
)

ML4_SCRIPT_WIN = str(
    PHASE6_WIN / "ml4" / "anomaly.py"
)

ML6_SCRIPT_WIN = str(
    PHASE6_WIN / "ml6" / "batch_score.py"
)

TEMP_SCRIPT_WIN = str(
    DE7_WIN / "de7_de6_runner_temp.py"
)


# =========================================================
# WSL PATHS
# =========================================================

RAW_DIR_WSL = (
    DE2_WSL / "data" / "raw"
)

REJECTED_DIR_WSL = (
    DE2_WSL / "data" / "rejected"
)

AUDIT_FILE_WSL = (
    DE2_WSL / "logs" / "ingestion_audit.csv"
)

ANALYTICS_DIR_WSL = (
    DE2_WSL
    / "data"
    / "analytics"
    / "hourly_grid_summary"
)

ANALYTICS_DIR_WIN = str(
    DE2_WIN
    / "data"
    / "analytics"
    / "hourly_grid_summary"
)

DASHBOARD_FILE_WSL = (
    DE2_WSL / "data" / "dashboard_summary.csv"
)

REFERENCE_FILE_WSL = (
    DE2_WSL
    / "data"
    / "reference"
    / "milano-grid.geojson"
)

WAREHOUSE_DB_WSL = (
    DE6_WSL
    / "warehouse"
    / "network_analytics.db"
)

STATUS_DIR_WSL = (
    DE7_WSL
    / "data"
    / "pipeline_status"
)

STATUS_FILE_WSL = (
    STATUS_DIR_WSL / "pipeline_status.json"
)

TEMP_SCRIPT_WSL = (
    DE7_WSL / "de7_de6_runner_temp.py"
)


# =========================================================
# HELPER — RUN WINDOWS PYTHON SCRIPT
# =========================================================

def run_windows_python(script_path_win):
    command = [
        "powershell.exe",
        "-NoProfile",
        "-Command",
        f"& '{PYTHON_EXE_WIN}' '{script_path_win}'",
    ]

    env = os.environ.copy()

    env["HADOOP_HOME"] = r"C:\hadoop"
    env["hadoop.home.dir"] = r"C:\hadoop"

    env["PATH"] = (
        r"C:\hadoop\bin;"
        + env.get("PATH", "")
    )

    env["PYSPARK_PYTHON"] = PYTHON_EXE_WIN
    env["PYSPARK_DRIVER_PYTHON"] = PYTHON_EXE_WIN

    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        env=env,
    )

    if result.stdout:
        print(result.stdout)

    if result.stderr:
        print(result.stderr)

    if result.returncode != 0:
        raise RuntimeError(
            f"Windows Python script failed: "
            f"{script_path_win}"
        )


# =========================================================
# TASK 1 — INGEST
# =========================================================

def ingest_task():
    print("=" * 70)
    print("DE7 TASK: INGEST")
    print("=" * 70)

    print(
        "DE7: Starting DE2 landing-to-raw ingestion"
    )

    run_windows_python(
        INGESTION_SCRIPT_WIN
    )

    print(
        "DE7: Ingestion completed successfully"
    )


# =========================================================
# TASK 2 — VALIDATE
# =========================================================

def validate_task(**context):
    print("=" * 70)
    print("DE7 TASK: VALIDATE")
    print("=" * 70)

    if not AUDIT_FILE_WSL.exists():
        raise FileNotFoundError(
            f"Missing ingestion audit: "
            f"{AUDIT_FILE_WSL}"
        )

    dag_run = context["dag_run"]

    expected_date = (
        dag_run.logical_date.strftime("%Y-%m-%d")
    )

    expected_filename = (
        f"sms-call-internet-mi-{expected_date}.csv"
    )

    expected_raw_file = (
        RAW_DIR_WSL / expected_filename
    )

    print(
        f"Expected daily file: "
        f"{expected_filename}"
    )

    if not expected_raw_file.exists():
        print(
            f"ERROR: Expected daily file not found: "
            f"{expected_raw_file}"
        )

        raise RuntimeError(
            "Validation failed: expected daily "
            f"file is missing: {expected_filename}"
        )

    raw_files = list(
        RAW_DIR_WSL.glob(
            "sms-call-internet-mi-*.csv"
        )
    )

    rejected_files = list(
        REJECTED_DIR_WSL.glob(
            "sms-call-internet-mi-*.csv"
        )
    )

    print(
        f"Raw files: {len(raw_files)}"
    )

    print(
        f"Rejected files: {len(rejected_files)}"
    )

    print(
        f"Audit file: {AUDIT_FILE_WSL}"
    )

    print(
        f"Expected file found: "
        f"{expected_raw_file}"
    )

    print("Validation passed.")


# =========================================================
# TASK 3 — SPARK PROCESS
# =========================================================

def spark_process_task():
    print("=" * 70)
    print("DE7 TASK: SPARK_PROCESS")
    print("=" * 70)

    run_windows_python(
        SPARK_SCRIPT_WIN
    )

    if not ANALYTICS_DIR_WSL.exists():
        raise RuntimeError(
            "Spark completed but analytics "
            "directory was not created."
        )

    if not DASHBOARD_FILE_WSL.exists():
        raise RuntimeError(
            "Spark completed but "
            "dashboard_summary.csv was not created."
        )

    print(
        "Spark processing completed."
    )


# =========================================================
# TASK 4 — LOAD WAREHOUSE
# =========================================================

def load_warehouse_task():
    print("=" * 70)
    print("DE7 TASK: LOAD_WAREHOUSE")
    print("=" * 70)

    if not ANALYTICS_DIR_WSL.exists():
        raise FileNotFoundError(
            f"Missing analytics output: "
            f"{ANALYTICS_DIR_WSL}"
        )

    warehouse_script_wsl = (
        DE6_WSL
        / "scripts"
        / "build_warehouse.py"
    )

    if not warehouse_script_wsl.exists():
        raise FileNotFoundError(
            f"Missing DE6 warehouse script: "
            f"{warehouse_script_wsl}"
        )

    original = warehouse_script_wsl.read_text(
        encoding="utf-8"
    )

    patched = original

    spark_python_config = f'''
import os

os.environ["PYSPARK_PYTHON"] = r"{PYTHON_EXE_WIN}"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"{PYTHON_EXE_WIN}"
os.environ["HADOOP_HOME"] = r"C:\\hadoop"
os.environ["PATH"] = (
    r"C:\\hadoop\\bin;"
    + os.environ.get("PATH", "")
)
'''

    patched = (
        spark_python_config
        + "\n"
        + patched
    )

    patched, source_count = re.subn(
        r'SOURCE_PATH\s*=\s*\(?\s*r"[^"]+"\s*r"[^"]+"\s*\)?',
        lambda _: (
            f'SOURCE_PATH = r"{ANALYTICS_DIR_WIN}"'
        ),
        patched,
        count=1,
    )

    patched, reference_count = re.subn(
        r'REFERENCE_PATH\s*=\s*\(?\s*r"[^"]+"\s*r"[^"]+"\s*\)?',
        lambda _: (
            "REFERENCE_PATH = "
            f'r"{str(DE2_WIN / "data" / "reference" / "milano-grid.geojson")}"'
        ),
        patched,
        count=1,
    )

    if source_count != 1:
        raise RuntimeError(
            "Could not redirect DE6 SOURCE_PATH."
        )

    if reference_count != 1:
        raise RuntimeError(
            "Could not redirect DE6 REFERENCE_PATH."
        )

    try:
        TEMP_SCRIPT_WSL.write_text(
            patched,
            encoding="utf-8"
        )

        print(
            "DE7: Running patched "
            "DE6 warehouse builder"
        )

        run_windows_python(
            TEMP_SCRIPT_WIN
        )

    finally:
        if TEMP_SCRIPT_WSL.exists():
            TEMP_SCRIPT_WSL.unlink()

    if not WAREHOUSE_DB_WSL.exists():
        raise RuntimeError(
            "Warehouse build completed but "
            "database was not found."
        )

    print(
        "Warehouse load completed."
    )


# =========================================================
# TASK 5 — ML2 FEATURE GENERATION
# =========================================================

def ml2_build_features_task():
    print("=" * 70)
    print("DE7 TASK: ML2_BUILD_FEATURES")
    print("=" * 70)

    if not WAREHOUSE_DB_WSL.exists():
        raise FileNotFoundError(
            f"Warehouse database missing: "
            f"{WAREHOUSE_DB_WSL}"
        )

    print(
        "DE7: Building ML2 network feature table"
    )

    run_windows_python(
        ML2_SCRIPT_WIN
    )

    with sqlite3.connect(
        str(WAREHOUSE_DB_WSL)
    ) as connection:

        table_exists = connection.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type = 'table'
              AND name = 'network_feature_table'
            """
        ).fetchone()

        if table_exists is None:
            raise RuntimeError(
                "ML2 completed but "
                "network_feature_table was not created."
            )

        row_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM network_feature_table
            """
        ).fetchone()[0]

    if row_count <= 0:
        raise RuntimeError(
            "ML2 feature table contains zero rows."
        )

    print(
        f"ML2 feature table ready: "
        f"{row_count} rows"
    )


# =========================================================
# TASK 6 — ML4 ANOMALY SCORING
# =========================================================

def ml4_anomaly_scoring_task():
    print("=" * 70)
    print("DE7 TASK: ML4_ANOMALY_SCORING")
    print("=" * 70)

    if not WAREHOUSE_DB_WSL.exists():
        raise FileNotFoundError(
            f"Warehouse database missing: "
            f"{WAREHOUSE_DB_WSL}"
        )

    print(
        "DE7: Running ML4 anomaly scorer"
    )

    run_windows_python(
        ML4_SCRIPT_WIN
    )

    with sqlite3.connect(
        str(WAREHOUSE_DB_WSL)
    ) as connection:

        table_exists = connection.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type = 'table'
              AND name = 'network_anomaly_scores'
            """
        ).fetchone()

        if table_exists is None:
            raise RuntimeError(
                "ML4 completed but "
                "network_anomaly_scores was not created."
            )

        row_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM network_anomaly_scores
            """
        ).fetchone()[0]

    if row_count <= 0:
        raise RuntimeError(
            "ML4 anomaly score table contains zero rows."
        )

    print(
        f"ML4 anomaly scores ready: "
        f"{row_count} rows"
    )


# =========================================================
# TASK 7 — ML6 BATCH RISK SCORING
# =========================================================

def ml6_batch_scoring_task():
    print("=" * 70)
    print("DE7 TASK: ML6_BATCH_SCORING")
    print("=" * 70)

    if not WAREHOUSE_DB_WSL.exists():
        raise FileNotFoundError(
            f"Warehouse database missing: "
            f"{WAREHOUSE_DB_WSL}"
        )

    model_path = (
        PHASE6_WIN
        / "ml3"
        / "outputs"
        / "risk_classifier.pkl"
    )

    if not model_path.exists():
        raise FileNotFoundError(
            f"ML3 trained model missing: "
            f"{model_path}"
        )

    print(
        "DE7: Running ML6 batch risk scorer"
    )

    run_windows_python(
        ML6_SCRIPT_WIN
    )

    with sqlite3.connect(
        str(WAREHOUSE_DB_WSL)
    ) as connection:

        table_exists = connection.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type = 'table'
              AND name = 'network_risk_scores'
            """
        ).fetchone()

        if table_exists is None:
            raise RuntimeError(
                "ML6 completed but "
                "network_risk_scores was not created."
            )

        row_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM network_risk_scores
            """
        ).fetchone()[0]

        unique_keys = connection.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT DISTINCT grid_id, feature_timestamp
                FROM network_risk_scores
            )
            """
        ).fetchone()[0]

        model_versions = connection.execute(
            """
            SELECT COUNT(DISTINCT model_version)
            FROM network_risk_scores
            """
        ).fetchone()[0]

    if row_count <= 0:
        raise RuntimeError(
            "ML6 risk score table contains zero rows."
        )

    if row_count != unique_keys:
        raise RuntimeError(
            "ML6 risk score table contains duplicate "
            "(grid_id, feature_timestamp) keys."
        )

    if model_versions != 1:
        raise RuntimeError(
            "ML6 risk score table contains multiple "
            "model versions."
        )

    print(
        f"ML6 risk scores ready: {row_count} rows"
    )

    print(
        f"ML6 unique keys: {unique_keys}"
    )

    print(
        f"ML6 model versions: {model_versions}"
    )


# =========================================================
# TASK 8 — QUALITY CHECK
# =========================================================

def quality_check_task(**context):
    print("=" * 70)
    print("DE7 TASK: QUALITY_CHECK")
    print("=" * 70)

    STATUS_DIR_WSL.mkdir(
        parents=True,
        exist_ok=True
    )

    dag_run = context["dag_run"]

    upstream_ids = [
        "ingest",
        "validate",
        "spark_process",
        "load_warehouse",
        "ml2_build_features",
        "ml4_anomaly_scoring",
        "ml6_batch_scoring",
    ]

    task_states = {}

    for task_id in upstream_ids:
        ti = TaskInstance.get_task_instance(
            dag_id=dag_run.dag_id,
            task_id=task_id,
            run_id=dag_run.run_id,
            map_index=-1,
        )

        task_states[task_id] = (
            ti.state
            if ti and ti.state
            else "unknown"
        )

    upstream_failed = any(
        task_states[task_id] != "success"
        for task_id in upstream_ids
    )

    rows_in = 0
    rows_rejected = 0
    nulls_handled = 0
    rows_published = 0
    feature_rows = 0
    anomaly_rows = 0
    risk_rows = 0
    as_of = None

    if DASHBOARD_FILE_WSL.exists():
        with open(
            DASHBOARD_FILE_WSL,
            "r",
            encoding="utf-8-sig",
            newline=""
        ) as f:

            dashboard_rows = list(
                csv.DictReader(f)
            )

        dashboard = {
            row["metric"]: row["value"]
            for row in dashboard_rows
        }

        rows_in = int(
            float(
                dashboard.get(
                    "raw_activity_rows",
                    0
                )
            )
        )

        rows_rejected = int(
            float(
                dashboard.get(
                    "rejected_records",
                    0
                )
            )
        )

        as_of = dashboard.get(
            "end_timestamp"
        )

    activity_columns = {
        "sms_in",
        "sms_out",
        "call_in",
        "call_out",
        "internet_activity",
    }

    if RAW_DIR_WSL.exists():
        for csv_file in RAW_DIR_WSL.glob(
            "sms-call-internet-mi-*.csv"
        ):

            with open(
                csv_file,
                "r",
                encoding="utf-8-sig",
                newline=""
            ) as f:

                reader = csv.DictReader(f)

                for row in reader:
                    for column in activity_columns:
                        value = row.get(column)

                        if (
                            value is None
                            or str(value).strip() == ""
                        ):
                            nulls_handled += 1

    if WAREHOUSE_DB_WSL.exists():
        connection = sqlite3.connect(
            str(WAREHOUSE_DB_WSL)
        )

        try:
            cursor = connection.cursor()

            rows_published = int(
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM fact_network_activity
                    """
                ).fetchone()[0]
            )

            feature_rows = int(
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM network_feature_table
                    """
                ).fetchone()[0]
            )

            anomaly_rows = int(
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM network_anomaly_scores
                    """
                ).fetchone()[0]
            )

            risk_rows = int(
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM network_risk_scores
                    """
                ).fetchone()[0]
            )

        finally:
            connection.close()

    if not as_of:
        raise RuntimeError(
            "Quality check failed: "
            "AS_OF is missing."
        )

    if feature_rows <= 0:
        raise RuntimeError(
            "Quality check failed: "
            "ML2 feature table is empty."
        )

    if anomaly_rows <= 0:
        raise RuntimeError(
            "Quality check failed: "
            "ML4 anomaly table is empty."
        )

    if risk_rows <= 0:
        raise RuntimeError(
            "Quality check failed: "
            "ML6 risk table is empty."
        )

    if upstream_failed:
        task_states["quality_check"] = "failure"
        pipeline_result = "failure"
    else:
        task_states["quality_check"] = "success"
        pipeline_result = "success"

    task_states["notify"] = "pending"

    status = {
        "run_id": dag_run.run_id,
        "run_timestamp": (
            datetime.now()
            .astimezone()
            .isoformat(
                timespec="seconds"
            )
        ),
        "tasks": task_states,
        "metrics": {
            "rows_in": rows_in,
            "rows_rejected": rows_rejected,
            "nulls_handled": nulls_handled,
            "rows_published": rows_published,
            "feature_rows": feature_rows,
            "anomaly_rows": anomaly_rows,
            "risk_rows": risk_rows,
        },
        "AS_OF": as_of,
        "pipeline_result": pipeline_result,
    }

    with open(
        STATUS_FILE_WSL,
        "w",
        encoding="utf-8"
    ) as f:

        json.dump(
            status,
            f,
            indent=2
        )

    print(
        json.dumps(
            status,
            indent=2
        )
    )

    if upstream_failed:
        raise RuntimeError(
            "Quality check failed because "
            "an upstream task failed."
        )

    print(
        "QUALITY CHECK PASSED."
    )


# =========================================================
# TASK 9 — NOTIFY
# =========================================================

def notify_task(**context):
    print("=" * 70)
    print("DE7 TASK: NOTIFY")
    print("=" * 70)

    dag_run = context["dag_run"]

    critical_tasks = [
        "ingest",
        "validate",
        "spark_process",
        "load_warehouse",
        "ml2_build_features",
        "ml4_anomaly_scoring",
        "ml6_batch_scoring",
        "quality_check",
    ]

    states = {}

    for task_id in critical_tasks:
        ti = TaskInstance.get_task_instance(
            dag_id=dag_run.dag_id,
            task_id=task_id,
            run_id=dag_run.run_id,
            map_index=-1,
        )

        states[task_id] = (
            ti.state
            if ti and ti.state
            else "unknown"
        )

    pipeline_success = all(
        state == "success"
        for state in states.values()
    )

    if pipeline_success:
        result = "SUCCESS"
        pipeline_result = "success"
    else:
        result = "FAILURE"
        pipeline_result = "failure"

    print("=" * 70)

    for task_id, state in states.items():
        print(
            f"{task_id}: {state}"
        )

    print("=" * 70)

    if STATUS_FILE_WSL.exists():
        with open(
            STATUS_FILE_WSL,
            "r",
            encoding="utf-8"
        ) as f:

            status = json.load(f)

    else:
        status = {
            "run_id": dag_run.run_id,
            "run_timestamp": (
                datetime.now()
                .astimezone()
                .isoformat(
                    timespec="seconds"
                )
            ),
            "tasks": {},
            "metrics": {},
            "AS_OF": None,
        }

    status.setdefault(
        "tasks",
        {}
    )

    status["tasks"].update(
        states
    )

    status["tasks"]["notify"] = (
        "success"
        if pipeline_success
        else "failure"
    )

    status["pipeline_result"] = (
        pipeline_result
    )

    status["notification_result"] = result

    with open(
        STATUS_FILE_WSL,
        "w",
        encoding="utf-8"
    ) as f:

        json.dump(
            status,
            f,
            indent=2
        )

    if pipeline_success:
        print(
            "DE7 NOTIFICATION: "
            "Pipeline completed successfully."
        )

    else:
        print(
            "DE7 NOTIFICATION: "
            "Pipeline failed."
        )

        raise RuntimeError(
            "DE7 pipeline failed."
        )


# =========================================================
# DAG
# =========================================================

with DAG(
    dag_id="de7_end_to_end_pipeline",

    start_date=datetime(
        2026,
        8,
        1
    ),

    schedule=None,

    catchup=False,

    tags=[
        "DE7",
        "end-to-end",
        "telecom",
        "ML2",
        "ML4",
        "ML6",
    ],
) as dag:

    ingest = PythonOperator(
        task_id="ingest",
        python_callable=ingest_task,
    )

    validate = PythonOperator(
        task_id="validate",
        python_callable=validate_task,
    )

    spark_process = PythonOperator(
        task_id="spark_process",
        python_callable=spark_process_task,

        retries=1,

        retry_delay=timedelta(
            seconds=10
        ),
    )

    load_warehouse = PythonOperator(
        task_id="load_warehouse",
        python_callable=load_warehouse_task,
    )

    ml2_build_features = PythonOperator(
        task_id="ml2_build_features",
        python_callable=ml2_build_features_task,
    )

    ml4_anomaly_scoring = PythonOperator(
        task_id="ml4_anomaly_scoring",
        python_callable=ml4_anomaly_scoring_task,
    )

    ml6_batch_scoring = PythonOperator(
        task_id="ml6_batch_scoring",
        python_callable=ml6_batch_scoring_task,
    )

    quality_check = PythonOperator(
        task_id="quality_check",
        python_callable=quality_check_task,

        trigger_rule=TriggerRule.ALL_DONE,
    )

    notify = PythonOperator(
        task_id="notify",
        python_callable=notify_task,

        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        ingest
        >> validate
        >> spark_process
        >> load_warehouse
        >> ml2_build_features
        >> ml4_anomaly_scoring
        >> ml6_batch_scoring
        >> quality_check
        >> notify
    )
