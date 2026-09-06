from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from ingestion.ingestion import detect_files, route_file


def detect_task(**context):
    """Detect incoming daily files."""

    files = detect_files()

    if not files:
        print("No candidate files found.")
        return

    filenames = [str(file) for file in files]

    context["ti"].xcom_push(
        key="detected_files",
        value=filenames
    )

    print(f"Detected {len(files)} file(s).")


def validate_and_route_task(**context):
    """Validate and route detected files."""

    files = context["ti"].xcom_pull(
        task_ids="detect_files",
        key="detected_files"
    )

    if not files:
        print("No files to process.")
        return

    for file_path in files:
        route_file(file_path)


with DAG(
    dag_id="de2_landing_to_raw_ingestion",
    start_date=datetime(2026, 8, 1),
    schedule=None,
    catchup=False,
    tags=["DE2", "ingestion"],
) as dag:

    detect_files_task = PythonOperator(
        task_id="detect_files",
        python_callable=detect_task,
    )

    validate_and_route = PythonOperator(
        task_id="validate_and_route",
        python_callable=validate_and_route_task,
    )

    detect_files_task >> validate_and_route