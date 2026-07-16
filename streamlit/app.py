import plotly.express as px
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import streamlit as st

# Cấu hình trang Dashboard
st.set_page_config(
    page_title="NYC Yellow Taxi Gold Analytics",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("🚖 NYC Yellow Taxi - Gold Layer Dashboard")
st.markdown(
    "Dashboard phân tích hiệu năng và doanh thu hệ thống Taxi NYC chạy trên nền tảng "
    "Spark Distributed Engine."
)


# -----------------------------------------------------------------------------
# 1. KHỞI TẠO SPARK SESSION (Chạy truy vấn phân tán)
# -----------------------------------------------------------------------------
@st.cache_resource
def get_spark_session():
    """Khởi tạo và cache SparkSession kết nối tới Spark Cluster."""
    return (
        SparkSession.builder.appName("streamlit-gold-analytics")
        # Kết nối tới distributed master đã cấu hình trong docker-compose
        .master("spark://spark-master:7077")
        # Cấu hình tối ưu tài nguyên cho app giao diện
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


try:
    spark = get_spark_session()
except Exception as e:
    st.error(f"Không thể kết nối tới Spark Cluster: {e}")
    st.stop()

# -----------------------------------------------------------------------------
# 2. ĐỌC DỮ LIỆU TỪ TẦNG GOLD (Parquet - Không dùng @st.cache_data tại đây)
# -----------------------------------------------------------------------------
GOLD_FACT_PATH = "/storage/gold/fact_trip_summary"
GOLD_DIM_PATH = "/storage/gold/dim_taxi_zone"


def load_gold_data():
    """Đọc dữ liệu từ tầng Gold và thực hiện JOIN phân tán trên Spark."""
    fact_df = spark.read.parquet(GOLD_FACT_PATH)
    dim_df = spark.read.parquet(GOLD_DIM_PATH)

    # Thực hiện phép JOIN phân tán (Distributed Query) để lấy thông tin vùng miền
    joined_df = fact_df.join(
        dim_df.withColumnRenamed("borough", "pickup_borough")
        .withColumnRenamed("zone", "pickup_zone")
        .withColumnRenamed("service_zone", "pickup_service_zone"),
        fact_df.pulocationid == dim_df.location_id,
        "left",
    ).drop("location_id")

    return joined_df


with st.spinner("🔄 Đang thiết lập liên kết dữ liệu trên Spark Cluster..."):
    # Spark Lazy Evaluation: Lệnh này chỉ build Logical Plan,
    # chưa thực thi tính toán nặng
    spark_gold_df = load_gold_data()

# -----------------------------------------------------------------------------
# 3. SIDEBAR - ĐIỀU HƯỚNG & PHÂN VÙNG DRILL-DOWN
# -----------------------------------------------------------------------------
st.sidebar.header("🛠️ Bộ lọc & Tùy chọn")

# Lấy danh sách các năm bằng Spark Action (distinct & collect) đưa về sidebar
with st.spinner("📥 Đang truy vấn danh sách năm khả dụng..."):
    available_years = [
        row["year"] for row in spark_gold_df.select("year").distinct().collect()
    ]
selected_year = st.sidebar.selectbox(
    "Chọn Năm phân tích", sorted(available_years, reverse=True)
)

# Lọc dữ liệu theo năm đã chọn trực tiếp trên Spark
filtered_spark_df = spark_gold_df.filter(F.col("year") == selected_year)

# Chọn Dimension để Drill-down cho biểu đồ Time Series
drill_down_dim = st.sidebar.selectbox(
    "Drill-down biểu đồ Thời Gian theo:",
    options=["is_weekend", "pickup_borough", "pickup_day_of_week"],
    index=0,
    format_func=lambda x: {
        "is_weekend": "Cuối tuần vs Ngày thường",
        "pickup_borough": "Quận (Borough)",
        "pickup_day_of_week": "Ngày trong tuần",
    }[x],
)

# -----------------------------------------------------------------------------
# 4. TRUY VẤN PHÂN TÁN SONG SONG (SPARK DISTRIBUTED AGGREGATIONS)
# -----------------------------------------------------------------------------
with st.spinner(
    "⚡ Spark đang thực thi các truy vấn gom cụm phân tán (Distributed Aggregation)..."
):

    # --- 4.1. Tính toán các chỉ số KPI Metrics ---
    # Tổng hợp toàn bộ chỉ số về 1 dòng duy nhất trên Spark trước khi collect
    kpi_spark_df = filtered_spark_df.select(
        F.sum("total_trips").alias("total_trips"),
        F.sum("total_revenue").alias("total_revenue"),
        F.sum("total_passengers").alias("total_passengers"),
        F.sum("total_tip_amount").alias("total_tip_amount"),
    )
    kpi_row = kpi_spark_df.collect()[
        0
    ]  # Chỉ thu thập duy nhất 1 dòng kết quả về driver

    total_trips = kpi_row["total_trips"] or 0
    total_revenue = kpi_row["total_revenue"] or 0.0
    total_passengers = kpi_row["total_passengers"] or 0
    total_tip_amount = kpi_row["total_tip_amount"] or 0.0
    avg_tip = (total_tip_amount / total_trips) if total_trips > 0 else 0.0

    # --- 4.2. Query 1: Xu hướng chuyến xe theo tháng (Time Series) ---
    time_series_pdf = (
        filtered_spark_df.groupBy("month", drill_down_dim)
        .agg(F.sum("total_trips").alias("total_trips"))
        .orderBy("month")
        .toPandas()  # Chỉ collect bảng kết quả cực nhỏ (tối đa vài chục dòng)
    )

    # --- 4.3. Query 2: Hiệu suất khu vực đón khách (Borough) ---
    borough_pdf = (
        filtered_spark_df.groupBy("pickup_borough")
        .agg(
            F.sum("total_revenue").alias("total_revenue"),
            F.sum("total_trips").alias("total_trips"),
        )
        .orderBy(F.desc("total_revenue"))
        .toPandas()  # Chỉ collect danh sách các quận (khoảng dưới 10 dòng)
    )

    # --- 4.4. Query 3: Hành vi khách hàng theo khung giờ ---
    hour_pdf = (
        filtered_spark_df.groupBy("pickup_hour")
        .agg(
            F.sum("total_trips").alias("total_trips"),
            # Sum quãng đường trước, chia trung bình sau để đảm bảo
            # tính chính xác toán học trên phân tán
            F.sum("total_distance_miles").alias("sum_distance"),
        )
        .orderBy("pickup_hour")
        .toPandas()  # Chỉ collect 24 dòng tương ứng với 24 giờ
    )
    # Tính quãng đường trung bình trên Pandas từ các cột đã được tổng hợp ở Spark
    hour_pdf["avg_distance"] = hour_pdf["sum_distance"] / hour_pdf["total_trips"]


# -----------------------------------------------------------------------------
# 5. HIỂN THỊ CÁC CHỈ SỐ TỔNG QUAN (KPI METRICS)
# -----------------------------------------------------------------------------
st.subheader(f"📊 Chỉ số tổng quan năm {selected_year}")
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Tổng số chuyến xe", f"{total_trips:,}")
with col2:
    st.metric("Tổng doanh thu ($)", f"{total_revenue:,.2f}")
with col3:
    st.metric("Tổng lượng khách", f"{total_passengers:,}")
with col4:
    st.metric("Tiền tip bình quân ($)", f"{avg_tip:,.2f}")

st.markdown("---")

# -----------------------------------------------------------------------------
# 6. HIỂN THỊ BIỂU ĐỒ (VISUALIZATION)
# -----------------------------------------------------------------------------
st.subheader("💡 Phân Tích & Trả Lời Câu Hỏi Doanh Nghiệp")

# Tách giao diện thành các cột lớn
left_chart, right_chart = st.columns(2)

with left_chart:
    # --- CHART 1 (Time Series với Drill-down) ---
    st.markdown("### 1. Xu hướng tổng số chuyến xe thay đổi như thế nào qua các tháng?")

    fig1 = px.line(
        time_series_pdf,
        x="month",
        y="total_trips",
        color=drill_down_dim,
        markers=True,
        labels={"month": "Tháng", "total_trips": "Số lượng chuyến xe"},
        title=f"Số chuyến xe theo từng tháng (Phân rã theo {drill_down_dim})",
    )
    fig1.update_layout(xaxis=dict(tickmode="linear", tick0=1, dtick=1))
    st.plotly_chart(fig1, use_container_width=True)

with right_chart:
    # --- CHART 2 (Hiệu suất khu vực đón khách) ---
    st.markdown("### 2. Quận (Borough) nào có lượng khách và doanh thu cao nhất?")

    fig2 = px.bar(
        borough_pdf,
        x="pickup_borough",
        y="total_revenue",
        hover_data=["total_trips"],
        labels={"pickup_borough": "Quận đón", "total_revenue": "Tổng doanh thu ($)"},
        title="Bảng xếp hạng doanh thu theo khu vực hành chính",
        color="total_revenue",
        color_continuous_scale="Viridis",
    )
    st.plotly_chart(fig2, use_container_width=True)

st.markdown("---")

# --- CHART 3 (Hành vi khách hàng theo giờ trong ngày) ---
st.markdown(
    "### 3. Khung giờ nào trong ngày thường có mật độ di chuyển cao nhất và hành khách "
    "đi xa hơn?"
)

fig3 = px.bar(
    hour_pdf,
    x="pickup_hour",
    y="total_trips",
    color="avg_distance",
    labels={
        "pickup_hour": "Giờ đón xe (0h - 23h)",
        "total_trips": "Tổng số lượng chuyến xe",
        "avg_distance": "Quãng đường TB (Miles)",
    },
    title="Phân bổ số lượng chuyến đi theo giờ trong ngày "
    "(Màu sắc thể hiện độ dài quãng đường)",
    color_continuous_scale="Plasma",
)
fig3.update_layout(xaxis=dict(tickmode="linear", tick0=0, dtick=2))
st.plotly_chart(fig3, use_container_width=True)

# -----------------------------------------------------------------------------
# 7. THÔNG TIN ENGINE KIỂM TRA
# -----------------------------------------------------------------------------
st.sidebar.markdown("---")
st.sidebar.info(
    "ℹ️ **Thông tin Kỹ thuật:**\n"
    "- **Engine:** Apache Spark 3.5 (Distributed Cluster)\n"
    "- **Master URL:** `spark://spark-master:7077`\n"
    "- **Data Source:** Parquet (Gold Layer)\n"
    "- **Query Execution:** Fully Distributed Aggregation"
)
