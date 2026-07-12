"""Transform yellow tripdata from Bronze to Silver layer.

This job performs data cleansing, validation rules, and feature enrichment
(e.g., trip duration, time attributes) to prepare high-quality data for the Silver zone.
"""

from __future__ import annotations

import argparse
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

DEFAULT_BRONZE_DIR = Path("/storage/bronze/trip")
DEFAULT_SILVER_DIR = Path("/storage/silver/trip")

logger = logging.getLogger(__name__)


def _spark_modules():
    from pyspark.sql import functions as F  # pyright: ignore[reportMissingImports]

    return F


def _is_already_transformed(silver_dir: Path, year: int, month: int) -> bool:
    """Kiểm tra xem phân vùng dữ liệu đã tồn tại ở tầng Silver chưa."""
    partition_path = silver_dir / f"year={year}" / f"month={month}"
    if partition_path.exists() and partition_path.is_dir():
        parquet_files = list(partition_path.glob("*.parquet"))
        return len(parquet_files) > 0
    return False


def _extract_bronze_partitions(bronze_dir: Path) -> list[tuple[int, int]]:
    """Quét thư mục Bronze để tìm danh sách các cặp (year, month) hiện có."""
    partitions = []
    if not bronze_dir.exists():
        return partitions

    for year_path in bronze_dir.glob("year=*"):
        if year_path.is_dir():
            try:
                year = int(year_path.name.split("=")[1])
                for month_path in year_path.glob("month=*"):
                    if month_path.is_dir():
                        month = int(month_path.name.split("=")[1])
                        partitions.append((year, month))
            except (IndexError, ValueError):
                continue
    return sorted(partitions)


def _clean_and_enrich_data(df):
    """Thực hiện các quy tắc làm sạch dữ liệu và tạo thêm tính năng (Enrichment)."""
    F = _spark_modules()

    # 1. Bộ lọc làm sạch dữ liệu (Data Cleansing Rules)
    cleaned_df = df.filter(
        (F.col("passenger_count") > 0)
        & (F.col("trip_distance") > 0.0)
        & (F.col("fare_amount") >= 0.0)
        & (F.col("total_amount") >= 0.0)
        & (F.col("pulocationid") > 0)
        & (F.col("dolocationid") > 0)
    )

    # 2. Làm giàu dữ liệu (Data Enrichment)
    enriched_df = (
        cleaned_df.withColumn(
            "trip_duration_minutes",
            (
                F.col("dropoff_datetime").cast("long")
                - F.col("pickup_datetime").cast("long")
            )
            / 60.0,
        )
        .withColumn("pickup_hour", F.hour(F.col("pickup_datetime")))
        .withColumn("pickup_day_of_week", F.dayofweek(F.col("pickup_datetime")))
        .withColumn(
            "is_weekend",
            F.when(F.col("pickup_day_of_week").isin(1, 7), F.lit(True)).otherwise(
                F.lit(False)
            ),
        )
    )

    final_df = enriched_df.filter(F.col("trip_duration_minutes") > 0.0)
    return final_df


def _project_silver_columns(frame):
    """Lựa chọn cấu trúc schema chuẩn hóa cuối cùng cho tầng Silver."""
    F = _spark_modules()
    return frame.select(
        "vendor_id",
        "vendor_name",
        "pickup_datetime",
        "dropoff_datetime",
        "trip_duration_minutes",
        "pickup_hour",
        "pickup_day_of_week",
        "is_weekend",
        "passenger_count",
        "trip_distance",
        "ratecode_id",
        "store_and_fwd_flag",
        "pulocationid",
        "dolocationid",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "congestion_surcharge",
        "airport_fee",
        "total_amount",
        "schema_family",
        "batch_id",
        "year",
        "month",
        F.current_timestamp().alias("transformed_at"),
    )


def transform_bronze_to_silver(
    spark,
    bronze_dir: Path = DEFAULT_BRONZE_DIR,
    silver_dir: Path = DEFAULT_SILVER_DIR,
    batch_id: str | None = None,
):
    """Đọc dữ liệu từ Bronze, làm sạch, biến đổi cấu trúc và ghi xuống Silver."""
    start_time = time.time()
    active_batch_id = (
        batch_id
        or f"silver-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-"
        f"{uuid4().hex[:8]}"
    )

    bronze_partitions = _extract_bronze_partitions(bronze_dir)
    if not bronze_partitions:
        logger.warning("No partitions found in bronze directory: %s", bronze_dir)
        return {"partitions_processed": 0, "batch_id": active_batch_id}

    logger.info(
        "status=START_SILVER_TRANSFORMATION batch_id=%s partitions_found=%d",
        active_batch_id,
        len(bronze_partitions),
    )

    total_records_written = 0
    partitions_skipped = 0

    for year, month in bronze_partitions:
        if _is_already_transformed(silver_dir, year, month):
            logger.info(
                "status=SKIP_PARTITION batch_id=%s partition='year=%d/month=%d' "
                "reason='Already exists in Silver'",
                active_batch_id,
                year,
                month,
            )
            partitions_skipped += 1
            continue

        partition_start_time = time.time()
        logger.info(
            "status=PROCESSING_PARTITION batch_id=%s partition='year=%d/month=%d'",
            active_batch_id,
            year,
            month,
        )

        partition_bronze_path = bronze_dir / f"year={year}" / f"month={month}"
        bronze_df = spark.read.parquet(str(partition_bronze_path))

        silver_enriched_df = _clean_and_enrich_data(bronze_df)
        silver_final_df = _project_silver_columns(silver_enriched_df)

        silver_final_df.cache()
        records_count = silver_final_df.count()
        total_records_written += records_count

        silver_final_df.write.mode("append").partitionBy("year", "month").parquet(
            str(silver_dir)
        )
        silver_final_df.unpersist()

        partition_duration = time.time() - partition_start_time
        logger.info(
            "status=FINISHED_PARTITION batch_id=%s partition='year=%d/month=%d' "
            "duration_sec=%.2f records_written=%d",
            active_batch_id,
            year,
            month,
            partition_duration,
            records_count,
        )

    job_duration = time.time() - start_time
    logger.info(
        "status=SUCCESS_SILVER batch_id=%s total_duration_sec=%.2f "
        "total_records_written=%d partitions_skipped=%d",
        active_batch_id,
        job_duration,
        total_records_written,
        partitions_skipped,
    )

    return {
        "batch_id": active_batch_id,
        "partitions_processed": len(bronze_partitions) - partitions_skipped,
        "records_written": total_records_written,
        "job_duration_sec": job_duration,
    }


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Transform data from Bronze to Silver layer"
    )
    parser.add_argument("--bronze-dir", type=Path, default=DEFAULT_BRONZE_DIR)
    parser.add_argument("--silver-dir", type=Path, default=DEFAULT_SILVER_DIR)
    parser.add_argument(
        "--app-name", type=str, default="nyc-taxi-silver-transformation"
    )
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
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.default.parallelism", "32")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    try:
        metrics = transform_bronze_to_silver(
            spark=spark,
            bronze_dir=args.bronze_dir,
            silver_dir=args.silver_dir,
            batch_id=args.batch_id,
        )
        print(metrics)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
