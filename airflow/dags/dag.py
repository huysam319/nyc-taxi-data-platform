"""Airflow DAG for yellow tripdata ingestion.

This DAG is intentionally lightweight. It delegates execution to a configurable
runtime command so the same DAG can work with spark-submit, docker exec, or any
other Spark launch mechanism your environment provides.
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

# Tách chuỗi dài để tái sử dụng mà không làm vượt quá 88 ký tự
SPARK_SUBMIT_CMD = (
    "/home/airflow/.local/bin/spark-submit "
    "--master spark://spark-master:7077 "
    "--driver-memory 4g "
    "--executor-memory 4g"
)

# Sử dụng phép cộng chuỗi tự nhiên của Python thay vì f-string
# Điều này giữ nguyên vẹn script Python gốc mà không lo bị lỗi nuốt dấu ngoặc nhọn {}
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
    env(
        "INGESTION_COMMAND",
        """
    + f'"{SPARK_SUBMIT_CMD}"'
    + r""",
    )
)
script = env("INGESTION_APP", "/opt/spark-apps/jobs/ingestion.py")
args = [
    "--app-name",
    env("INGESTION_APP_NAME", "nyc-taxi-ingestion"),
    "--landing-dir",
    env("INGESTION_LANDING_DIR", "/storage/landing/trip"),
    "--bronze-dir",
    env("INGESTION_BRONZE_DIR", "/storage/bronze/trip"),
    "--quarantine-dir",
    env("INGESTION_QUARANTINE_DIR", "/storage/bronze/trip/_quarantine"),
    "--batch-id",
    env("INGESTION_BATCH_ID", "manual-trigger"),
]

full_command = command + [script] + args
print("Running:", " ".join(full_command))
subprocess.run(full_command, check=True)
PY
"""
)


with DAG(
    "yellow_tripdata_ingestion",
    default_args=DEFAULT_ARGS,
    description="Ingest yellow taxi tripdata from landing into bronze",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["nyc-taxi", "ingestion", "spark"],
) as dag:
    ingest_tripdata = BashOperator(
        task_id="ingest_yellow_tripdata",
        bash_command=INGESTION_PYTHON_SNIPPET,
        env={
            "INGESTION_COMMAND": (
                "{{ ((dag_run.conf or {}).get('ingestion_command')) "
                f"or var.value.get('ingestion_command', '{SPARK_SUBMIT_CMD}') }}"
            ),
            "INGESTION_APP": (
                "{{ ((dag_run.conf or {}).get('ingestion_app')) "
                "or '/opt/spark-apps/jobs/ingestion.py' }}"
            ),
            "INGESTION_APP_NAME": (
                "{{ ((dag_run.conf or {}).get('app_name')) "
                "or 'nyc-taxi-ingestion' }}"
            ),
            "INGESTION_LANDING_DIR": (
                "{{ ((dag_run.conf or {}).get('landing_dir')) "
                "or '/storage/landing/trip' }}"
            ),
            "INGESTION_BRONZE_DIR": (
                "{{ ((dag_run.conf or {}).get('bronze_dir')) "
                "or '/storage/bronze/trip' }}"
            ),
            "INGESTION_QUARANTINE_DIR": (
                "{{ ((dag_run.conf or {}).get('quarantine_dir')) "
                "or '/storage/bronze/trip/_quarantine' }}"
            ),
            "INGESTION_BATCH_ID": (
                "{{ ((dag_run.conf or {}).get('batch_id')) " "or dag_run.run_id }}"
            ),
        },
    )
