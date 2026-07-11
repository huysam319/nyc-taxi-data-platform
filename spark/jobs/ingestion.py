"""Ingest yellow tripdata parquet files from landing into bronze.

The job normalizes the NYC yellow taxi schema (pre-2015), quarantines schema-violating rows, 
and preserves valid rows in Bronze. It includes a check to skip already ingested year/month data.
"""

from __future__ import annotations

import argparse
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


YEAR_MONTH_PATTERN = re.compile(r"yellow_tripdata_(\d{4})-(\d{2})\.parquet$")

DEFAULT_LANDING_DIR = Path("/storage/landing/trip")
DEFAULT_BRONZE_DIR = Path("/storage/bronze/trip")
DEFAULT_QUARANTINE_DIR = DEFAULT_BRONZE_DIR / "_quarantine"

MODERN_REQUIRED_COLUMNS = [
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "pulocationid",
    "dolocationid",
    "fare_amount",
    "total_amount",
]

logger = logging.getLogger(__name__)


def _spark_modules():
    from pyspark.sql import functions as F  # pyright: ignore[reportMissingImports]
    return F


def _iter_trip_files(input_dir: Path) -> list[Path]:
    files = sorted(input_dir.glob("yellow_tripdata_*.parquet"))
    return [file_path for file_path in files if file_path.is_file()]


def _parse_year_month(file_path: Path) -> tuple[int, int]:
    match = YEAR_MONTH_PATTERN.search(file_path.name)
    if not match:
        raise ValueError(f"Unsupported yellow tripdata filename: {file_path.name}")

    return int(match.group(1)), int(match.group(2))


def _is_already_ingested(bronze_dir: Path, year: int, month: int) -> bool:
    """KIỂM TRA ĐIỀU KIỆN: Xác định xem phân vùng dữ liệu của year/month đã tồn tại ở Bronze chưa."""
    # Khớp chính xác với cấu trúc thư mục mà Spark .partitionBy("year", "month") tạo ra
    partition_path = bronze_dir / f"year={year}" / f"month={month}"
    
    # Nếu thư mục phân vùng tồn tại và có chứa ít nhất một file dữ liệu .parquet bên trong
    if partition_path.exists() and partition_path.is_dir():
        parquet_files = list(partition_path.glob("*.parquet"))
        return len(parquet_files) > 0
        
    return False


def _safe_value(columns: set[str], column_name: str, data_type: str):
    F = _spark_modules()
    if column_name in columns:
        return F.col(column_name).cast(data_type)

    return F.lit(None).cast(data_type)


def _schema_violation_reason(required_columns: list[str]):
    F = _spark_modules()
    missing_columns = [
        F.when(F.col(column_name).isNull(), F.lit(column_name))
        for column_name in required_columns
    ]
    return F.trim(F.concat_ws(", ", *missing_columns))


def _normalize_modern_frame(df, year: int, month: int):
    F = _spark_modules()
    columns = set(df.columns)

    normalized = df.select(
        F.lit(None).cast("string").alias("vendor_name"),
        _safe_value(columns, "VendorID", "int").alias("vendor_id"),
        _safe_value(columns, "tpep_pickup_datetime", "timestamp").alias("pickup_datetime"),
        _safe_value(columns, "tpep_dropoff_datetime", "timestamp").alias("dropoff_datetime"),
        _safe_value(columns, "passenger_count", "int").alias("passenger_count"),
        _safe_value(columns, "trip_distance", "double").alias("trip_distance"),
        _safe_value(columns, "RatecodeID", "int").alias("ratecode_id"),
        _safe_value(columns, "store_and_fwd_flag", "string").alias("store_and_fwd_flag"),
        F.lit(None).cast("double").alias("pickup_longitude"),
        F.lit(None).cast("double").alias("pickup_latitude"),
        F.lit(None).cast("double").alias("dropoff_longitude"),
        F.lit(None).cast("double").alias("dropoff_latitude"),
        _safe_value(columns, "PULocationID", "int").alias("pulocationid"),
        _safe_value(columns, "DOLocationID", "int").alias("dolocationid"),
        _safe_value(columns, "payment_type", "int").alias("payment_type"),
        _safe_value(columns, "fare_amount", "double").alias("fare_amount"),
        _safe_value(columns, "extra", "double").alias("extra"),
        _safe_value(columns, "mta_tax", "double").alias("mta_tax"),
        _safe_value(columns, "tip_amount", "double").alias("tip_amount"),
        _safe_value(columns, "tolls_amount", "double").alias("tolls_amount"),
        _safe_value(columns, "improvement_surcharge", "double").alias("improvement_surcharge"),
        _safe_value(columns, "congestion_surcharge", "double").alias("congestion_surcharge"),
        _safe_value(columns, "airport_fee", "double").alias("airport_fee"),
        _safe_value(columns, "total_amount", "double").alias("total_amount"),
        F.lit("modern").alias("schema_family"),
        F.col("source_file"),
        F.lit(year).cast("int").alias("year"),
        F.lit(month).cast("int").alias("month"),
        F.current_timestamp().alias("ingested_at"),
    )

    return normalized.withColumn(
        "schema_violation_reason",
        _schema_violation_reason(MODERN_REQUIRED_COLUMNS),
    )


def _split_valid_and_invalid(frame):
    F = _spark_modules()
    invalid = frame.filter(F.length(F.col("schema_violation_reason")) > 0)
    valid = frame.filter(F.length(F.col("schema_violation_reason")) == 0)
    return valid, invalid


def _add_batch_metadata(frame, batch_id: str):
    F = _spark_modules()
    return frame.withColumn("batch_id", F.lit(batch_id))


def _project_clean_columns(frame):
    return frame.select(
        "vendor_name", "vendor_id", "pickup_datetime", "dropoff_datetime",
        "passenger_count", "trip_distance", "ratecode_id", "store_and_fwd_flag",
        "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude",
        "pulocationid", "dolocationid", "payment_type", "fare_amount", "extra",
        "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
        "congestion_surcharge", "airport_fee", "total_amount", "schema_family",
        "batch_id", "source_file", "year", "month", "ingested_at",
    )


def _project_failure_columns(frame):
    return frame.select(
        "vendor_name", "vendor_id", "pickup_datetime", "dropoff_datetime",
        "passenger_count", "trip_distance", "ratecode_id", "store_and_fwd_flag",
        "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude",
        "pulocationid", "dolocationid", "payment_type", "fare_amount", "extra",
        "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
        "congestion_surcharge", "airport_fee", "total_amount", "schema_family",
        "batch_id", "source_file", "year", "month", "ingested_at",
        "failure_mode", "failure_reason",
    )


def ingest_yellow_tripdata(
    spark,
    landing_dir: Path = DEFAULT_LANDING_DIR,
    bronze_dir: Path = DEFAULT_BRONZE_DIR,
    quarantine_dir: Path = DEFAULT_QUARANTINE_DIR,
    batch_id: str | None = None,
):
    """Ingest all yellow tripdata parquet files from landing to bronze.

    The job normalizes the NYC yellow taxi schema, quarantines schema-violating 
    records, and preserves clean rows in Bronze. Skips files already ingested.
    """
    start_time = time.time()
    F = _spark_modules()
    active_batch_id = batch_id or f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid4().hex[:8]}"
    landing_files = _iter_trip_files(landing_dir)
    
    if not landing_files:
        raise FileNotFoundError(f"No yellow tripdata parquet files found in {landing_dir}")

    logger.info("status=START batch_id=%s landing_files=%s", active_batch_id, len(landing_files))

    total_records_processed = 0
    total_schema_violations = 0
    files_skipped = 0

    for file_path in landing_files:
        year, month = _parse_year_month(file_path)
        
        # ÁP DỤNG ĐIỀU KIỆN KIỂM TRA: Nếu đã được kết nạp rồi thì bỏ qua không xử lý lại
        if _is_already_ingested(bronze_dir, year, month):
            logger.info("status=SKIP_FILE batch_id=%s file=%s reason='Year=%d Month=%d already exists in Bronze'", 
                        active_batch_id, file_path.name, year, month)
            files_skipped += 1
            continue

        file_start_time = time.time()
        logger.info("status=PROCESSING_FILE batch_id=%s file=%s", active_batch_id, file_path.name)
        
        source_df = spark.read.parquet(str(file_path)).withColumn(
            "source_file", F.input_file_name()
        )

        normalized = _normalize_modern_frame(source_df, year, month)
        normalized = _add_batch_metadata(normalized, active_batch_id)
        
        valid_frame, invalid_frame = _split_valid_and_invalid(normalized)
        
        valid_frame.cache()
        invalid_frame.cache()

        file_records = valid_frame.count()
        file_violations = invalid_frame.count()
        
        total_records_processed += file_records
        total_schema_violations += file_violations

        # 1. Ghi dữ liệu sạch trực tiếp vào Bronze
        bronze_frame = _project_clean_columns(valid_frame)
        bronze_frame.write.mode("append").partitionBy("year", "month").parquet(str(bronze_dir))

        # 2. Ghi lỗi Schema vào Quarantine
        invalid_to_write = _project_failure_columns(
            invalid_frame.withColumn("failure_mode", F.lit("schema_violation")).withColumn(
                "failure_reason", F.col("schema_violation_reason")
            )
        )
        invalid_to_write.write.mode("append").partitionBy("year", "month").parquet(
            str(quarantine_dir / "schema_violations")
        )

        valid_frame.unpersist()
        invalid_frame.unpersist()

        file_duration = time.time() - file_start_time
        logger.info(
            "status=FINISHED_FILE batch_id=%s file=%s duration_sec=%.2f clean_records=%d violations=%d",
            active_batch_id, file_path.name, file_duration, file_records, file_violations
        )

    job_duration = time.time() - start_time
    
    logger.info(
        "status=SUCCESS batch_id=%s total_duration_sec=%.2f total_clean_records=%d total_schema_violations=%d files_skipped=%d",
        active_batch_id, job_duration, total_records_processed, total_schema_violations, files_skipped
    )

    return {
        "landing_files": len(landing_files),
        "files_skipped": files_skipped,
        "batch_id": active_batch_id,
        "records_processed": total_records_processed,
        "job_duration_sec": job_duration
    }


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingest yellow tripdata into bronze")
    parser.add_argument("--landing-dir", type=Path, default=DEFAULT_LANDING_DIR)
    parser.add_argument("--bronze-dir", type=Path, default=DEFAULT_BRONZE_DIR)
    parser.add_argument("--quarantine-dir", type=Path, default=DEFAULT_QUARANTINE_DIR)
    parser.add_argument("--app-name", type=str, default="nyc-taxi-ingestion")
    parser.add_argument("--batch-id", type=str, default=None)
    return parser


def main() -> None:
    from pyspark.sql import SparkSession  # pyright: ignore[reportMissingImports]

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    parser = build_argument_parser()
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName(args.app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.files.maxPartitionBytes", "128MB")
        .getOrCreate()
    )
    try:
        counts = ingest_yellow_tripdata(
            spark=spark,
            landing_dir=args.landing_dir,
            bronze_dir=args.bronze_dir,
            quarantine_dir=args.quarantine_dir,
            batch_id=args.batch_id,
        )
        print(counts)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()