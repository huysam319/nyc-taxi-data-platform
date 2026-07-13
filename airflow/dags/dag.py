"""Airflow DAG for yellow tripdata ingestion and silver transformation.

This DAG is intentionally lightweight. It delegates execution to configurable
runtime commands so it can work nicely with any Spark launch mechanisms.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator  # pyright: ignore[reportMissingImports]

from airflow import DAG

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Cấu hình cụm lệnh Spark submit dùng chung để tránh lỗi dòng quá dài
SPARK_SUBMIT_BASE = (
    "/home/airflow/.local/bin/spark-submit "
    "--master spark://spark-master:7077 "
    "--driver-memory 4g "
    "--executor-memory 4g"
)

# -------------------------------------------------------------------------
# 1. SNIPPET PYTHON CHO BƯỚC INGESTION
# -------------------------------------------------------------------------
INGESTION_PYTHON_SNIPPET = (
    r"""
python - <<'PY'
import os
import shlex
import subprocess


def env(name: str, default: str) -> str:
    value = os.environ.get(name, default)
    return value

command = shlex.split(
    env("INGESTION_COMMAND", """
    + f'"{SPARK_SUBMIT_BASE}"'
    + r""",)
)
script = env("INGESTION_APP", "/opt/spark-apps/jobs/ingestion.py")
args = [
    "--app-name", env("INGESTION_APP_NAME", "nyc-taxi-ingestion"),
    "--landing-dir", env("INGESTION_LANDING_DIR", "/storage/landing/trip"),
    "--bronze-dir", env("INGESTION_BRONZE_DIR", "/storage/bronze/trip"),
    "--quarantine-dir", 
        env("INGESTION_QUARANTINE_DIR", "/storage/bronze/trip/_quarantine"),
    "--batch-id", env("INGESTION_BATCH_ID", "manual-trigger"),
]

full_command = command + [script] + args
print("Running Ingestion:", " ".join(full_command))
subprocess.run(full_command, check=True)
PY
"""
)

# -------------------------------------------------------------------------
# 2. SNIPPET PYTHON CHO BƯỚC TRANSFORMATION SILVER
# -------------------------------------------------------------------------
SILVER_PYTHON_SNIPPET = (
    r"""
python - <<'PY'
import os
import shlex
import subprocess


def env(name: str, default: str) -> str:
    value = os.environ.get(name, default)
    return value

command = shlex.split(
    env("SILVER_COMMAND", """
    + f'"{SPARK_SUBMIT_BASE}"'
    + r""",)
)
script = env("SILVER_APP", "/opt/spark-apps/jobs/transformation_silver.py")
args = [
    "--bronze-dir", env("SILVER_BRONZE_DIR", "/storage/bronze/trip"),
    "--silver-dir", env("SILVER_SILVER_DIR", "/storage/silver/trip"),
]

full_command = command + [script] + args
print("Running Silver Transformation:", " ".join(full_command))
subprocess.run(full_command, check=True)
PY
"""
)


with DAG(
    "yellow_tripdata_pipeline",
    default_args=DEFAULT_ARGS,
    description="Pipeline to ingest and transform yellow taxi tripdata",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["nyc-taxi", "medallion", "spark"],
) as dag:
    # TASK 1: INGESTION (Landing -> Bronze)
    ingest_tripdata = BashOperator(
        task_id="ingest_yellow_tripdata",
        bash_command=INGESTION_PYTHON_SNIPPET,
        env={
            "INGESTION_COMMAND": "{{ ((dag_run.conf or {}).get('ingestion_command')) "
            "or var.value.get('ingestion_command', "
            "'/home/airflow/.local/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "--driver-memory 4g --executor-memory 4g') }}",
            "INGESTION_APP": "{{ ((dag_run.conf or {}).get('ingestion_app')) "
            "or '/opt/spark-apps/jobs/ingestion.py' }}",
            "INGESTION_APP_NAME": "{{ ((dag_run.conf or {}).get('app_name')) "
            "or 'nyc-taxi-ingestion' }}",
            "INGESTION_LANDING_DIR": "{{ ((dag_run.conf or {}).get('landing_dir')) "
            "or '/storage/landing/trip' }}",
            "INGESTION_BRONZE_DIR": "{{ ((dag_run.conf or {}).get('bronze_dir')) "
            "or '/storage/bronze/trip' }}",
            "INGESTION_QUARANTINE_DIR": "{{ ((dag_run.conf "
            "or {}).get('quarantine_dir')) "
            "or '/storage/bronze/trip/_quarantine' }}",
            "INGESTION_BATCH_ID": "{{ ((dag_run.conf or {}).get('batch_id')) "
            "or dag_run.run_id }}",
        },
    )

    # TASK 2: TRANSFORMATION SILVER (Bronze -> Silver)
    transform_silver = BashOperator(
        task_id="transform_yellow_tripdata_silver",
        bash_command=SILVER_PYTHON_SNIPPET,
        env={
            "SILVER_COMMAND": "{{ ((dag_run.conf or {}).get('silver_command')) "
            "or var.value.get('silver_command', "
            "'/home/airflow/.local/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "--driver-memory 4g --executor-memory 4g') }}",
            "SILVER_APP": (
                "{{ ((dag_run.conf or {}).get('silver_app')) "
                + "or '/opt/spark-apps/jobs/transformation_silver.py' }}"
            ),
            "SILVER_BRONZE_DIR": (
                "{{ ((dag_run.conf or {}).get('bronze_dir')) "
                + "or '/storage/bronze/trip' }}"
            ),
            "SILVER_SILVER_DIR": (
                "{{ ((dag_run.conf or {}).get('silver_dir')) "
                + "or '/storage/silver/trip' }}"
            ),
            # # Lấy thông số Year/Month linh hoạt dựa trên cấu hình Trigger
            # "SILVER_YEAR": (
            #     "{{ ((dag_run.conf or {}).get('year')) or '2015' }}"
            # ),
            # "SILVER_MONTH": (
            #     "{{ ((dag_run.conf or {}).get('month')) or '1' }}"
            # ),
        },
    )

    # Thiết lập thứ tự chạy tuần tự: Ingest xong mới Transform Silver
    ingest_tripdata >> transform_silver
