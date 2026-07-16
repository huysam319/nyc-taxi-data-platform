"""Transform yellow tripdata from Silver to Gold layer using Star Schema.

This job creates:
- dim_taxi_zone (Dimension table)
- fact_trip_summary (Fact table - Aggregated)
"""

from __future__ import annotations

import argparse
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

DEFAULT_SILVER_DIR = Path("/storage/silver/trip")
DEFAULT_LOOKUP_FILE = Path("/storage/reference/taxi_zone_lookup.csv")
DEFAULT_GOLD_DIR = Path("/storage/gold")


def _spark_modules():
    from pyspark.sql import functions as F  # pyright: ignore[reportMissingImports]

    return F


def build_dimension_zone(spark, lookup_file_path: Path, gold_dir: Path):
    """Xây dựng bảng Dimension: dim_taxi_zone."""
    F = _spark_modules()
    logger.info("Building dim_taxi_zone from %s", lookup_file_path)

    # Đọc file CSV tra cứu vị trí
    lookup_df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(str(lookup_file_path))
    )

    # Chuẩn hóa tên cột chuẩn Snake_case theo quy chuẩn Data Engineer
    dim_zone_df = lookup_df.select(
        F.col("LocationID").alias("location_id"),
        F.col("Borough").alias("borough"),
        F.col("Zone").alias("zone"),
        F.col("service_zone").alias("service_zone"),
    )

    # Ghi đè vào tầng Gold (Dữ liệu danh mục nhỏ nên gom về 1 file duy nhất)
    dim_zone_path = gold_dir / "dim_taxi_zone"
    dim_zone_df.coalesce(1).write.mode("overwrite").parquet(str(dim_zone_path))
    logger.info("Successfully populated dim_taxi_zone at %s", dim_zone_path)


def _aggregate_fact_metrics(silver_df):
    """Hàm xử lý non-trivial aggregations để tính toán các chỉ số Fact."""
    F = _spark_modules()

    # Nhóm theo các Khía cạnh phân tích (Dimensions) và Phân vùng (Partition keys)
    fact_df = silver_df.groupBy(
        "year",
        "month",
        "pickup_hour",
        "pickup_day_of_week",
        "is_weekend",
        "pulocationid",
        "dolocationid",
    ).agg(
        F.count("vendor_id").alias("total_trips"),
        F.sum("passenger_count").alias("total_passengers"),
        F.sum("trip_distance").alias("total_distance_miles"),
        F.avg("trip_distance").alias("avg_distance_miles"),
        F.avg("trip_duration_minutes").alias("avg_duration_minutes"),
        F.sum("fare_amount").alias("total_fare_amount"),
        F.sum("tip_amount").alias("total_tip_amount"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("total_amount").alias("avg_cost_per_trip"),
    )
    return fact_df


def transform_single_gold_partition(
    spark, year: int, month: int, silver_dir: Path, gold_dir: Path, batch_id: str
) -> bool:
    """Xử lý tạo Fact Table cho một phân vùng tháng cụ thể."""
    F = _spark_modules()
    partition_silver_path = silver_dir / f"year={year}" / f"month={month}"

    if not partition_silver_path.exists():
        logger.warning(
            "Partition not found in silver directory: %s", partition_silver_path
        )
        return False

    logger.info("Processing fact_trip_summary for year=%d/month=%d", year, month)

    # 1. Đọc dữ liệu từ Silver
    silver_df = spark.read.parquet(str(partition_silver_path))

    # 2. Xử lý Aggregation tinh thể hóa tầng Gold
    fact_aggregated_df = _aggregate_fact_metrics(silver_df)

    # 3. Làm giàu Siêu dữ liệu quản trị batch
    fact_final_df = fact_aggregated_df.withColumn(
        "batch_id", F.lit(batch_id)
    ).withColumn("generated_at", F.current_timestamp())

    # 4. Gom cụm tối ưu hóa kích thước file nén tầng Gold (tránh phình Small Files)
    fact_final_df = fact_final_df.coalesce(1)

    # 5. Đường dẫn đích của phân vùng Fact
    partition_gold_fact_path = (
        gold_dir / "fact_trip_summary" / f"year={year}" / f"month={month}"
    )

    # 6. Ghi đĩa dữ liệu Fact dạng Parquet
    fact_final_df.write.mode("overwrite").parquet(str(partition_gold_fact_path))
    return True


def transform_silver_to_gold(
    spark,
    year: int | None = None,
    month: int | None = None,
    silver_dir: Path = DEFAULT_SILVER_DIR,
    lookup_file: Path = DEFAULT_LOOKUP_FILE,
    gold_dir: Path = DEFAULT_GOLD_DIR,
    batch_id: str | None = None,
):
    """Hàm điều phối pipeline từ Silver sang Gold."""
    start_time = time.time()
    active_batch_id = batch_id or (
        f"gold-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-"
        f"{uuid4().hex[:8]}"
    )

    # Bước A: Xây dựng hoặc cập nhật bảng danh mục Dimension cố định
    if lookup_file.exists():
        build_dimension_zone(spark, lookup_file, gold_dir)
    else:
        logger.warning("Lookup file missing. Skipping dim_taxi_zone build.")

    # Bước B: Xử lý Fact Table theo phân vùng
    if year is not None and month is not None:
        target_partitions = [(year, month)]
    else:
        # Tự động quét phân vùng thư mục Silver nếu chạy thủ công
        target_partitions = []
        if silver_dir.exists():
            for y_path in silver_dir.glob("year=*"):
                for m_path in y_path.glob("month=*"):
                    try:
                        y = int(y_path.name.split("=")[1])
                        m = int(m_path.name.split("=")[1])
                        target_partitions.append((y, m))
                    except (IndexError, ValueError):
                        continue
        target_partitions = sorted(target_partitions)

    if not target_partitions:
        logger.warning("No partitions found to process for Fact Table.")
        return {"partitions_processed": 0, "batch_id": active_batch_id}

    processed_count = 0
    for y, m in target_partitions:
        if transform_single_gold_partition(
            spark, y, m, silver_dir, gold_dir, active_batch_id
        ):
            processed_count += 1

    duration = time.time() - start_time
    logger.info(
        "status=SUCCESS_GOLD batch_id=%s duration=%.2fs processed=%d",
        active_batch_id,
        duration,
        processed_count,
    )
    return {
        "batch_id": active_batch_id,
        "partitions_processed": processed_count,
        "duration_sec": duration,
    }


def main() -> None:
    from pyspark.sql import SparkSession  # pyright: ignore[reportMissingImports]

    parser = argparse.ArgumentParser(
        description="Gold Layer Data Modeling (Star Schema)"
    )
    parser.add_argument("--silver-dir", type=Path, default=DEFAULT_SILVER_DIR)
    parser.add_argument("--lookup-file", type=Path, default=DEFAULT_LOOKUP_FILE)
    parser.add_argument("--gold-dir", type=Path, default=DEFAULT_GOLD_DIR)
    parser.add_argument("--year", type=int, default=None)
    parser.add_argument("--month", type=int, default=None)
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("yellow-taxi-gold-modeling")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    try:
        transform_silver_to_gold(
            spark=spark,
            silver_dir=args.silver_dir,
            lookup_file=args.lookup_file,
            gold_dir=args.gold_dir,
            year=args.year,
            month=args.month,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
