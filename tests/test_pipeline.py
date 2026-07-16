# tests/test_pipeline.py
from datetime import datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark.jobs.transformation_gold import (
    _aggregate_fact_metrics,
    build_dimension_zone,
    transform_single_gold_partition,
)

# Import các hàm cốt lõi từ thư mục code của bạn
from spark.jobs.transformation_silver import (
    _clean_and_enrich_data,
    _project_silver_columns,
    transform_single_partition,
)


@pytest.fixture(scope="session")
def spark():
    """Khởi tạo một Local SparkSession tối ưu chạy trên RAM cho toàn bộ session test."""
    session = (
        SparkSession.builder.master("local[2]")
        .appName("nyc-taxi-pipeline-test")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


# =============================================================================
# CHỮA ĐỀ: 5 UNIT TESTS CHO TRANSFORMATIONS (SILVER & GOLD)
# =============================================================================


def test_unit_silver_cleansing(spark):
    """Test 1: Kiểm tra quy tắc lọc sạch dữ liệu của Silver (Loại bỏ giá trị vô lý)"""
    schema = StructType(
        [
            StructField("passenger_count", IntegerType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("pulocationid", IntegerType(), True),
            StructField("dolocationid", IntegerType(), True),
            StructField("pickup_datetime", TimestampType(), True),
            StructField("dropoff_datetime", TimestampType(), True),
        ]
    )

    # 1 dòng chuẩn, 1 dòng sai passenger, 1 dòng sai distance, 1 dòng sai location
    t_start = datetime(2026, 7, 16, 12, 0, 0)
    t_end = datetime(2026, 7, 16, 12, 15, 0)
    data = [
        (2, 2.5, 15.0, 18.0, 140, 141, t_start, t_end),  # Hợp lệ
        (
            0,
            1.2,
            10.0,
            11.0,
            140,
            141,
            t_start,
            t_end,
        ),  # passenger_count <= 0 (Sẽ bị loại)
        (1, 0.0, 5.0, 6.0, 140, 141, t_start, t_end),  # trip_distance <= 0 (Sẽ bị loại)
        (1, 4.0, 20.0, 25.0, 0, 141, t_start, t_end),  # pulocationid <= 0 (Sẽ bị loại)
    ]
    df = spark.createDataFrame(data, schema)

    cleaned_df = _clean_and_enrich_data(df, year=2026, month=7)

    # Chỉ giữ lại duy nhất 1 dòng hợp lệ
    assert cleaned_df.count() == 1


def test_unit_silver_enrichment_features(spark):
    """Test 2: Kiểm tra việc tính toán trip_duration_minutes và xác định is_weekend"""
    schema = StructType(
        [
            StructField("passenger_count", IntegerType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("pulocationid", IntegerType(), True),
            StructField("dolocationid", IntegerType(), True),
            StructField("pickup_datetime", TimestampType(), True),
            StructField("dropoff_datetime", TimestampType(), True),
        ]
    )

    # Ngày 16/07/2026 là Thứ Năm (pickup_day_of_week = 5, is_weekend = False)
    weekday_pickup = datetime(2026, 7, 16, 8, 0, 0)
    weekday_dropoff = datetime(2026, 7, 16, 8, 15, 0)  # 15 phút di chuyển

    # Ngày 19/07/2026 là Chủ Nhật (pickup_day_of_week = 1, is_weekend = True)
    weekend_pickup = datetime(2026, 7, 19, 10, 0, 0)
    weekend_dropoff = datetime(2026, 7, 19, 10, 30, 0)  # 30 phút di chuyển

    data = [
        (1, 3.0, 10.0, 12.0, 10, 20, weekday_pickup, weekday_dropoff),
        (2, 5.5, 20.0, 24.0, 10, 20, weekend_pickup, weekend_dropoff),
    ]
    df = spark.createDataFrame(data, schema)

    enriched_df = _clean_and_enrich_data(df, year=2026, month=7)
    rows = enriched_df.orderBy("pickup_datetime").collect()

    # Kiểm tra dòng ngày thường
    assert rows[0]["trip_duration_minutes"] == 15.0
    assert rows[0]["pickup_hour"] == 8
    assert rows[0]["is_weekend"] is False

    # Kiểm tra dòng cuối tuần
    assert rows[1]["trip_duration_minutes"] == 30.0
    assert rows[1]["is_weekend"] is True


def test_unit_silver_projection_schema(spark):
    """Test 3: Kiểm tra cấu trúc schema đầu ra của tầng Silver phải khớp tuyệt đối"""
    schema = StructType(
        [
            StructField("vendor_id", IntegerType(), True),
            StructField("pickup_datetime", TimestampType(), True),
            StructField("dropoff_datetime", TimestampType(), True),
            StructField("trip_duration_minutes", DoubleType(), True),
            StructField("pickup_hour", IntegerType(), True),
            StructField("pickup_day_of_week", IntegerType(), True),
            StructField("is_weekend", StringType(), True),
            StructField("passenger_count", IntegerType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("ratecode_id", IntegerType(), True),
            StructField("store_and_fwd_flag", StringType(), True),
            StructField("pulocationid", IntegerType(), True),
            StructField("dolocationid", IntegerType(), True),
            StructField("payment_type", IntegerType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("extra", DoubleType(), True),
            StructField("mta_tax", DoubleType(), True),
            StructField("tip_amount", DoubleType(), True),
            StructField("tolls_amount", DoubleType(), True),
            StructField("improvement_surcharge", DoubleType(), True),
            StructField("congestion_surcharge", DoubleType(), True),
            StructField("airport_fee", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("year", IntegerType(), True),
            StructField("month", IntegerType(), True),
        ]
    )

    # Tạo dữ liệu mock tối thiểu
    data = [
        (
            1,
            datetime(2026, 7, 16, 12, 0),
            datetime(2026, 7, 16, 12, 10),
            10.0,
            12,
            5,
            "False",
            1,
            2.0,
            1,
            "N",
            100,
            101,
            1,
            10.0,
            0.5,
            0.5,
            2.0,
            0.0,
            0.3,
            2.5,
            0.0,
            15.8,
            2026,
            7,
        )
    ]
    df = spark.createDataFrame(data, schema)

    # Định nghĩa mock vendor_name tĩnh
    df = df.withColumn("vendor_name", df["vendor_id"].cast("string"))

    projected_df = _project_silver_columns(df, batch_id="test-batch-123")

    # Kiểm tra cột bổ sung 'batch_id' và 'transformed_at'
    assert "batch_id" in projected_df.columns
    assert "transformed_at" in projected_df.columns
    assert projected_df.collect()[0]["batch_id"] == "test-batch-123"


def test_unit_gold_dimension_zone(spark, tmp_path):
    """Test 4: Kiểm tra việc xây dựng bảng dim_taxi_zone từ file CSV tra cứu"""
    # Khởi tạo file CSV mock
    csv_content = "LocationID,Borough,Zone,service_zone\n"
    csv_content += "1,Manhattan,EWR,Yellow\n2,Queens,Astoria,Boro\n"
    csv_file = tmp_path / "taxi_zone_lookup.csv"
    csv_file.write_text(csv_content)

    gold_dir = tmp_path / "gold"
    build_dimension_zone(spark, csv_file, gold_dir)

    # Xác thực tệp parquet được tạo ra thành công
    dim_parquet_path = gold_dir / "dim_taxi_zone"
    assert dim_parquet_path.exists()

    dim_df = spark.read.parquet(str(dim_parquet_path))
    assert dim_df.count() == 2
    assert "location_id" in dim_df.columns
    assert "borough" in dim_df.columns


def test_unit_gold_metrics_aggregation(spark):
    """Test 5: Kiểm tra tính phi phiếm (non-trivial)
    của việc Aggregation lên Fact Table"""
    schema = StructType(
        [
            StructField("year", IntegerType(), True),
            StructField("month", IntegerType(), True),
            StructField("pickup_hour", IntegerType(), True),
            StructField("pickup_day_of_week", IntegerType(), True),
            StructField("is_weekend", StringType(), True),
            StructField("pulocationid", IntegerType(), True),
            StructField("dolocationid", IntegerType(), True),
            StructField("vendor_id", StringType(), True),
            StructField("passenger_count", IntegerType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("trip_duration_minutes", DoubleType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("tip_amount", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
        ]
    )

    # Gộp 2 dòng thuộc cùng một nhóm phân tích (Dimension combo) để tính Aggregation
    data = [
        (2026, 7, 8, 5, "False", 100, 101, "vendor_A", 2, 3.0, 15.0, 12.0, 3.0, 15.0),
        (2026, 7, 8, 5, "False", 100, 101, "vendor_B", 1, 5.0, 25.0, 18.0, 5.0, 23.0),
    ]
    df = spark.createDataFrame(data, schema)

    fact_df = _aggregate_fact_metrics(df)
    row = fact_df.collect()[0]

    # Kiểm tra toán học phân tán
    assert row["total_trips"] == 2
    assert row["total_passengers"] == 3
    assert row["total_distance_miles"] == 8.0
    assert row["avg_distance_miles"] == 4.0
    assert row["avg_duration_minutes"] == 20.0
    assert row["total_revenue"] == 38.0  # 15.0 + 23.0


# =============================================================================
# INTEGRATION TEST (END-TO-END PIPELINE)
# =============================================================================


def test_integration_pipeline_e2e(spark, tmp_path):
    """Test 6: Kiểm thử luồng tích hợp hệ thống từ Bronze -> Silver -> Gold"""
    temp_dir = Path(tmp_path)
    bronze_dir = temp_dir / "bronze"
    silver_dir = temp_dir / "silver"
    gold_dir = temp_dir / "gold"

    # Tạo thư mục phân vùng giả lập cho Bronze
    partition_path = bronze_dir / "year=2026" / "month=7"
    partition_path.mkdir(parents=True, exist_ok=True)

    # 1. Khởi tạo dữ liệu mock cho Bronze Parquet
    bronze_schema = StructType(
        [
            StructField("vendor_id", StringType(), True),
            StructField("vendor_name", StringType(), True),
            StructField("pickup_datetime", TimestampType(), True),
            StructField("dropoff_datetime", TimestampType(), True),
            StructField("passenger_count", IntegerType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("ratecode_id", IntegerType(), True),
            StructField("store_and_fwd_flag", StringType(), True),
            StructField("pulocationid", IntegerType(), True),
            StructField("dolocationid", IntegerType(), True),
            StructField("payment_type", IntegerType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("extra", DoubleType(), True),
            StructField("mta_tax", DoubleType(), True),
            StructField("tip_amount", DoubleType(), True),
            StructField("tolls_amount", DoubleType(), True),
            StructField("improvement_surcharge", DoubleType(), True),
            StructField("congestion_surcharge", DoubleType(), True),
            StructField("airport_fee", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
        ]
    )

    bronze_data = [
        (
            "1",
            "Creative_Mobile_Technologies",
            datetime(2026, 7, 16, 8, 30),
            datetime(2026, 7, 16, 8, 45),
            1,
            4.2,
            1,
            "N",
            100,
            101,
            1,
            15.0,
            0.5,
            0.5,
            3.0,
            0.0,
            0.3,
            2.5,
            0.0,
            21.8,
        )
    ]
    spark.createDataFrame(bronze_data, bronze_schema).write.parquet(str(partition_path))

    # 2. RUN STEP 1: Bronze -> Silver
    batch_id_silver = "test-silver-batch"
    silver_success = transform_single_partition(
        spark=spark,
        year=2026,
        month=7,
        bronze_dir=bronze_dir,
        silver_dir=silver_dir,
        batch_id=batch_id_silver,
    )
    assert silver_success is True

    # Xác thực Silver đã sinh tệp chuẩn hóa
    silver_parquet = silver_dir / "year=2026" / "month=7"
    assert silver_parquet.exists()
    assert len(list(silver_parquet.glob("*.parquet"))) > 0

    # 3. RUN STEP 2: Silver -> Gold
    batch_id_gold = "test-gold-batch"
    gold_success = transform_single_gold_partition(
        spark=spark,
        year=2026,
        month=7,
        silver_dir=silver_dir,
        gold_dir=gold_dir,
        batch_id=batch_id_gold,
    )
    assert gold_success is True

    # Xác thực Gold Fact Table đã tính toán xong
    gold_fact_parquet = gold_dir / "fact_trip_summary" / "year=2026" / "month=7"
    assert gold_fact_parquet.exists()

    # Đối chiếu kết quả Fact cuối cùng thu được
    fact_df = spark.read.parquet(str(gold_fact_parquet))
    fact_row = fact_df.collect()[0]

    assert fact_row["total_trips"] == 1
    assert fact_row["total_revenue"] == 21.8
    assert fact_row["avg_duration_minutes"] == 15.0
    assert fact_row["batch_id"] == "test-gold-batch"
