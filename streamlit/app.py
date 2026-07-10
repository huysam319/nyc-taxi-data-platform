import os

import pandas as pd
import psycopg2

import streamlit as st

st.set_page_config(
    page_title="NYC Taxi Data Platform",
    page_icon="🚕",
    layout="wide",
)

st.title("NYC Taxi Data Platform")
st.caption("Streamlit dashboard cơ bản kết nối PostgreSQL")

with st.sidebar:
    st.header("Database connection")
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    dbname = os.getenv("POSTGRES_DB", "nyc_taxi")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "")

    st.write("Host:", host)
    st.write("Port:", port)
    st.write("Database:", dbname)
    st.write("User:", user)

st.subheader("Tổng quan")
col1, col2, col3 = st.columns(3)
col1.metric("Status", "Running")
col2.metric("App", "Streamlit")
col3.metric("DB", dbname)

st.divider()

st.subheader("Kiểm tra kết nối PostgreSQL")

query = st.text_area(
    "SQL query",
    value=(
        "SELECT current_timestamp AS current_time, "
        "current_database() AS database_name;"
    ),
    height=120,
)

if st.button("Run query", type="primary"):
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
        )

        df = pd.read_sql_query(query, conn)
        conn.close()

        st.success("Kết nối thành công")
        st.dataframe(df, use_container_width=True)

    except Exception as exc:
        st.error("Không thể kết nối hoặc chạy truy vấn.")
        st.exception(exc)

st.divider()

st.subheader("Hướng dẫn nhanh")
st.markdown("""
- Đảm bảo container PostgreSQL đang chạy.
- Biến môi trường phải khớp với file `.env`.
- Nếu muốn test nhanh, dùng query mặc định ở trên.
""")
