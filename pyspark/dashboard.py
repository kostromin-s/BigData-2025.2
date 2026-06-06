"""
Streamlit Dashboard - Thống kê bất động sản từ HDFS.
Chạy: streamlit run dashboard.py  (cổng 8501)
"""
from datetime import datetime

import streamlit as st
import pandas as pd
import plotly.express as px
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

import config

st.set_page_config(page_title="Real Estate Analytics", layout="wide")


@st.cache_resource
def get_spark():
    return (
        SparkSession.builder
        .appName("RealEstateDashboard")
        .master(config.SPARK_MASTER)
        .config("spark.hadoop.fs.defaultFS", config.HDFS_NAMENODE)
        .getOrCreate()
    )


@st.cache_data(ttl=300)
def load_pandas():
    """Đọc Parquet từ HDFS, chọn cột cần thiết, đưa về Pandas."""
    spark = get_spark()
    try:
        df = spark.read.parquet(config.HDFS_INPUT_PATH)
    except Exception as e:
        st.error(f"Không đọc được HDFS: {e}")
        return None
    cols = ["list_id", "title", "listing_type", "property_type", "price", "area_m2",
            "rooms", "district", "region", "url"]
    df = df.select([c for c in cols if c in df.columns]).filter(col("price").isNotNull())
    pdf = df.toPandas()
    if pdf.empty:
        return pdf
    pdf["price_ty"] = pdf["price"] / 1e9
    pdf["price_per_m2_trieu"] = (pdf["price"] / pdf["area_m2"]) / 1e6
    return pdf


def main():
    st.title("🏠 Real Estate Big Data Analytics")
    st.caption(f"Cập nhật: {datetime.now():%Y-%m-%d %H:%M:%S}")

    with st.spinner("Đang tải dữ liệu từ HDFS..."):
        pdf_all = load_pandas()

    if pdf_all is None or pdf_all.empty:
        st.warning("Chưa có dữ liệu. Hãy chạy crawler → push Kafka → Spark consumer trước.")
        return

    with st.sidebar:
        st.header("Điều khiển")
        if st.button("🔄 Tải lại dữ liệu", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
        ltype = "Bán"
        if "listing_type" in pdf_all.columns:
            options = ["Bán", "Cho thuê", "Tất cả"]
            ltype = st.radio("Hình thức", options, index=0)
        st.info(f"HDFS: {config.HDFS_NAMENODE}\n\nPath: {config.HDFS_INPUT_PATH}")

    # Lọc theo hình thức (giá bán & giá thuê khác đơn vị nên không trộn chung)
    if ltype != "Tất cả" and "listing_type" in pdf_all.columns:
        pdf = pdf_all[pdf_all["listing_type"] == ltype].copy()
    else:
        pdf = pdf_all.copy()
    if pdf.empty:
        st.warning(f"Không có tin nào thuộc hình thức: {ltype}")
        return
    st.caption(f"Đang xem: **{ltype}** · {len(pdf):,} tin")

    # ---- Metrics tổng quan ----
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Tổng số tin", f"{len(pdf):,}")
    c2.metric("Số khu vực", f"{pdf['district'].nunique():,}")
    if ltype == "Cho thuê":
        c3.metric("Giá thuê TB", f"{pdf['price'].mean()/1e6:.1f} tr/th")
    else:
        c3.metric("Giá TB", f"{pdf['price_ty'].mean():.2f} tỷ")
    if pdf["area_m2"].notna().any():
        c4.metric("Diện tích TB", f"{pdf['area_m2'].mean():.0f} m²")

    st.markdown("---")

    # ---- Biểu đồ ----
    t1, t2, t3, t4 = st.tabs(
        ["Theo khu vực", "Theo loại hình", "Phân bố giá", "Bảng chi tiết"]
    )

    with t1:
        by_dist = (pdf[pdf["district"] != ""]
                   .groupby("district")
                   .agg(n=("list_id", "count"), avg_price=("price_ty", "mean"))
                   .reset_index().sort_values("n", ascending=False).head(config.TOP_N))
        st.plotly_chart(
            px.bar(by_dist, x="n", y="district", orientation="h",
                   title="Số tin theo khu vực", labels={"n": "Số tin", "district": ""}),
            use_container_width=True)
        st.plotly_chart(
            px.bar(by_dist.sort_values("avg_price"), x="avg_price", y="district",
                   orientation="h", title="Giá trung bình theo khu vực (tỷ VND)",
                   labels={"avg_price": "Giá TB (tỷ)", "district": ""}),
            use_container_width=True)

    with t2:
        by_type = (pdf.groupby("property_type")
                   .size().reset_index(name="n").sort_values("n", ascending=False))
        st.plotly_chart(
            px.pie(by_type, values="n", names="property_type",
                   title="Tỷ trọng theo loại hình"), use_container_width=True)

    with t3:
        clip = pdf[pdf["price_ty"] < pdf["price_ty"].quantile(0.95)]
        st.plotly_chart(
            px.histogram(clip, x="price_ty", nbins=40,
                         title="Phân bố giá (tỷ VND, cắt 5% đuôi trên)",
                         labels={"price_ty": "Giá (tỷ VND)"}),
            use_container_width=True)
        scatter = pdf[(pdf["area_m2"].notna()) & (pdf["area_m2"] > 0)]
        scatter = scatter[scatter["area_m2"] < scatter["area_m2"].quantile(0.95)]
        st.plotly_chart(
            px.scatter(scatter, x="area_m2", y="price_ty", color="property_type",
                       title="Giá theo diện tích",
                       labels={"area_m2": "Diện tích (m²)", "price_ty": "Giá (tỷ)"}),
            use_container_width=True)

    with t4:
        st.dataframe(
            pdf[["title", "property_type", "district", "price_ty", "area_m2", "rooms", "url"]]
            .rename(columns={"title": "Tiêu đề", "property_type": "Loại hình",
                             "district": "Khu vực", "price_ty": "Giá (tỷ)",
                             "area_m2": "DT (m²)", "rooms": "Phòng", "url": "Link"}),
            use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()