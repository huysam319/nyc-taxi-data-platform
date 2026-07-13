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

# Cấu hình logging cơ bản nếu chạy độc lập
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

DEFAULT_BRONZE_DIR = Path("/storage/bronze/trip")
DEFAULT_SILVER_DIR = Path("/storage/silver/trip")


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


def _clean_and_enrich_data(df, year: int, month: int):
    """Thực hiện các quy tắc làm sạch dữ liệu và tạo thêm tính năng (Enrichment)."""
    F = _spark_modules()

    # Thêm thủ công cột tĩnh 'year', 'month' và 'batch_id'
    # (Do gộp chung luồng hoặc đọc trực tiếp từng phân vùng nên cần ép nhãn dữ liệu)
    df_with_bounds = df.withColumn("year", F.lit(year)).withColumn(
        "month", F.lit(month)
    )

    # 1. Bộ lọc làm sạch dữ liệu (Data Cleansing Rules)
    cleaned_df = df_with_bounds.filter(
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


def _project_silver_columns(frame, batch_id: str):
    """Lựa chọn cấu trúc schema chuẩn hóa cuối cùng cho tầng Silver."""
    F = _spark_modules()

    # Kiểm tra kiểm soát các cột ảo đặc thù nếu chưa có sẵn trong frame nguồn
    if "schema_family" not in frame.columns:
        frame = frame.withColumn("schema_family", F.lit("yellow_tripdata"))

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
        F.lit(batch_id).alias("batch_id"),
        "year",
        "month",
        F.current_timestamp().alias("transformed_at"),
    )


def transform_single_partition(
    spark, year: int, month: int, bronze_dir: Path, silver_dir: Path, batch_id: str
) -> bool:
    """Xử lý xử lý duy nhất một phân vùng cụ thể (Hàm lõi an toàn RAM)."""
    partition_bronze_path = bronze_dir / f"year={year}" / f"month={month}"

    if not partition_bronze_path.exists():
        logger.warning(
            "Partition not found in bronze directory: %s", partition_bronze_path
        )
        return False

    if _is_already_transformed(silver_dir, year, month):
        logger.info(
            "status=SKIP_PARTITION batch_id=%s partition='year=%d/month=%d' " \
            "reason='Already exists in Silver'",
            batch_id,
            year,
            month,
        )
        return False

    logger.info(
        "status=PROCESSING_PARTITION batch_id=%s partition='year=%d/month=%d'",
        batch_id,
        year,
        month,
    )

    # Đọc - Biến đổi - Ghi trực tiếp (Pipeline luồng không cache giúp tránh OOM)
    bronze_df = spark.read.parquet(str(partition_bronze_path))
    silver_enriched_df = _clean_and_enrich_data(bronze_df, year, month)
    silver_final_df = _project_silver_columns(silver_enriched_df, batch_id)

    # Chia nhỏ dữ liệu phân vùng đích để tránh phình dung lượng RAM khi thực thi ghi đĩa
    silver_final_df = silver_final_df.repartition(4)

    partition_silver_path = silver_dir / f"year={year}" / f"month={month}"
    silver_final_df.write.mode("overwrite").parquet(str(partition_silver_path))
    return True


def transform_bronze_to_silver(
    spark,
    year: int | None = None,
    month: int | None = None,
    bronze_dir: Path = DEFAULT_BRONZE_DIR,
    silver_dir: Path = DEFAULT_SILVER_DIR,
    batch_id: str | None = None,
):
    """Điều phối xử lý: Nhận trực tiếp cặp năm/tháng từ Airflow 
    hoặc tự động quét toàn thư mục."""
    start_time = time.time()
    active_batch_id = (
        batch_id
        or f"silver-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-"
        f"{uuid4().hex[:8]}"
    )

    # Quyết định danh sách phân vùng cần xử lý
    if year is not None and month is not None:
        target_partitions = [(year, month)]
    else:
        logger.info("No specific partition provided. Scanning bronze directory...")
        target_partitions = _extract_bronze_partitions(bronze_dir)

    if not target_partitions:
        logger.warning("No partitions to process for bronze directory: %s", bronze_dir)
        return {"partitions_processed": 0, "batch_id": active_batch_id}

    logger.info(
        "status=START_SILVER_TRANSFORMATION batch_id=%s total_partitions=%d",
        active_batch_id,
        len(target_partitions),
    )

    partitions_processed_count = 0
    for y, m in target_partitions:
        success = transform_single_partition(
            spark, y, m, bronze_dir, silver_dir, active_batch_id
        )
        if success:
            partitions_processed_count += 1

    job_duration = time.time() - start_time
    logger.info(
        "status=SUCCESS_SILVER batch_id=%s total_duration_sec=%.2f " \
        "partitions_processed=%d",
        active_batch_id,
        job_duration,
        partitions_processed_count,
    )

    return {
        "batch_id": active_batch_id,
        "partitions_processed": partitions_processed_count,
        "job_duration_sec": job_duration,
    }


def main() -> None:
    from pyspark.sql import SparkSession  # pyright: ignore[reportMissingImports]

    parser = argparse.ArgumentParser(
        description="Transform yellow tripdata into silver"
    )
    parser.add_argument("--bronze-dir", type=Path, default=DEFAULT_BRONZE_DIR)
    parser.add_argument("--silver-dir", type=Path, default=DEFAULT_SILVER_DIR)

    # Cho phép nhận vào year và month tùy chọn từ Airflow, không bắt buộc 
    # (required=False) để có thể quét tự động khi chạy tay
    parser.add_argument("--year", type=int, required=False, default=None)
    parser.add_argument("--month", type=int, required=False, default=None)
    args = parser.parse_args()

    # Khởi tạo Spark Session tối ưu bộ nhớ đệm
    spark = (
        SparkSession.builder.appName("yellow-taxi-silver-transformation")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )

    try:
        transform_bronze_to_silver(
            spark=spark,
            bronze_dir=args.bronze_dir,
            silver_dir=args.silver_dir,
            year=args.year,
            month=args.month,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
