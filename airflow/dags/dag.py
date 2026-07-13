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

# -------------------------------------------------------------------------
# 3. SNIPPET PYTHON CHO BƯỚC TRANSFORMATION GOLD (STAR SCHEMA MODELING)
# -------------------------------------------------------------------------
GOLD_PYTHON_SNIPPET = (
    r"""
python - <<'PY'
import os
import shlex
import subprocess

def env(name: str, default: str) -> str:
    return os.environ.get(name, default)

command = shlex.split(env("GOLD_COMMAND", """
    + f'"{SPARK_SUBMIT_BASE}"'
    + r"""))
script = env("GOLD_APP", "/opt/spark-apps/jobs/transformation_gold.py")

args = [
    "--silver-dir", env("GOLD_SILVER_DIR", "/storage/silver/trip"),
    "--lookup-file", env("GOLD_LOOKUP_FILE", "/storage/reference/taxi_zone_lookup.csv"),
    "--gold-dir", env("GOLD_GOLD_DIR", "/storage/gold"),
]

# Thêm tham số phân vùng động tương tự tầng Silver để xử lý tối ưu
year = env("GOLD_YEAR", "")
month = env("GOLD_MONTH", "")
if year and year != "None":
    args.extend(["--year", year])
if month and month != "None":
    args.extend(["--month", month])

full_command = command + [script] + args
print("Running Gold Data Modeling:", " ".join(full_command))
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
        },
    )

    # TASK 3: TRANSFORMATION GOLD (Silver -> Gold Data Marts)
    transform_gold = BashOperator(
        task_id="transform_yellow_tripdata_gold",
        bash_command=GOLD_PYTHON_SNIPPET,
        env={
            "GOLD_COMMAND": "{{ (dag_run.conf or {}).get('gold_command') or '"
            + SPARK_SUBMIT_BASE
            + "' }}",
            "GOLD_APP": "{{ (dag_run.conf or {}).get('gold_app') "
            "or '/opt/spark-apps/jobs/transformation_gold.py' }}",
            "GOLD_SILVER_DIR": "{{ (dag_run.conf or {}).get('silver_dir') "
            "or '/storage/silver/trip' }}",
            "GOLD_LOOKUP_FILE": "{{ (dag_run.conf or {}).get('lookup_file') "
            "or '/storage/reference/taxi_zone_lookup.csv' }}",
            "GOLD_GOLD_DIR": "{{ (dag_run.conf or {}).get('gold_dir') "
            "or '/storage/gold' }}",
            "GOLD_YEAR": "{{ (dag_run.conf or {}).get('year') }}",
            "GOLD_MONTH": "{{ (dag_run.conf or {}).get('month') }}",
        },
    )

    # Thiết lập thứ tự chạy tuần tự: Ingest xong mới Transform Silver
    ingest_tripdata >> transform_silver >> transform_gold
