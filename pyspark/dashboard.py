"""
Streamlit Dashboard - Thống kê bất động sản từ HDFS.
Chạy: streamlit run dashboard.py  (cổng 8501)
"""
from datetime import datetime

import numpy as np
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
            "rooms", "district", "region", "latitude", "longitude", "url"]
    df = df.select([c for c in cols if c in df.columns]).filter(col("price").isNotNull())
    return df.toPandas()


def main():
    st.title("🏠 Phân tích Bất động sản Hà Nội")
    st.caption(f"Thời điểm xem: {datetime.now():%Y-%m-%d %H:%M:%S}")

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

    # ---- Chọn đơn vị hiển thị theo hình thức ----
    # Bán  : giá = tỷ VND,        giá/m² = triệu/m²
    # Thuê : giá = triệu/tháng,   giá/m² = nghìn/m²/tháng
    is_rent = (ltype == "Cho thuê")
    if is_rent:
        pdf["price_disp"] = pdf["price"] / 1e6
        price_unit, price_axis = "triệu/th", "Giá (triệu/tháng)"
        ppm2_div, ppm2_unit, ppm2_axis = 1e3, "nghìn/m²/th", "Giá/m² (nghìn/m²/tháng)"
    else:
        pdf["price_disp"] = pdf["price"] / 1e9
        price_unit, price_axis = "tỷ", "Giá (tỷ VND)"
        ppm2_div, ppm2_unit, ppm2_axis = 1e6, "triệu/m²", "Giá/m² (triệu/m²)"

    # Giá/m² (chỉ tính khi diện tích hợp lệ)
    pdf["ppm2"] = np.where(
        pdf["area_m2"].fillna(0) > 0,
        (pdf["price"] / pdf["area_m2"]) / ppm2_div,
        np.nan,
    )

    st.caption(f"Đang xem: **{ltype}** · {len(pdf):,} tin")

    # ---- Metrics tổng quan ----
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Tổng số tin", f"{len(pdf):,}")
    c2.metric("Số khu vực", f"{pdf['district'].nunique():,}")
    # Dùng TRUNG VỊ (median) — chuẩn ngành BĐS, kháng tin giá rác/outlier
    c3.metric("Giá trung vị", f"{pdf['price_disp'].median():.2f} {price_unit}")
    ppm2_valid = pdf["ppm2"].dropna()
    c4.metric("Giá/m² trung vị", f"{ppm2_valid.median():.1f} {ppm2_unit}" if len(ppm2_valid) else "—")
    c5.metric("Diện tích TB",
              f"{pdf['area_m2'].mean():.0f} m²" if pdf["area_m2"].notna().any() else "—")

    st.markdown("---")

    # ---- Biểu đồ ----
    t1, t2, t3, t4, t5 = st.tabs(
        ["Theo khu vực", "Theo loại hình", "Phân bố giá", "Bản đồ", "Bảng chi tiết"]
    )

    with t1:
        by_dist = (pdf[pdf["district"] != ""]
                   .groupby("district")
                   .agg(n=("list_id", "count"), med_ppm2=("ppm2", "median"))
                   .reset_index().sort_values("n", ascending=False).head(config.TOP_N))
        st.plotly_chart(
            px.bar(by_dist, x="n", y="district", orientation="h",
                   title="Số tin theo khu vực", labels={"n": "Số tin", "district": ""}),
            use_container_width=True)
        st.plotly_chart(
            px.bar(by_dist.dropna(subset=["med_ppm2"]).sort_values("med_ppm2"),
                   x="med_ppm2", y="district", orientation="h",
                   title=f"Giá/m² trung vị theo khu vực ({ppm2_unit})",
                   labels={"med_ppm2": ppm2_axis, "district": ""}),
            use_container_width=True)

    with t2:
        by_type = (pdf.groupby("property_type")
                   .size().reset_index(name="n").sort_values("n", ascending=False))
        st.plotly_chart(
            px.pie(by_type, values="n", names="property_type",
                   title="Tỷ trọng theo loại hình"), use_container_width=True)

    with t3:
        clip = pdf[pdf["price_disp"] < pdf["price_disp"].quantile(0.95)]
        st.plotly_chart(
            px.histogram(clip, x="price_disp", nbins=40,
                         title="Phân bố giá (cắt 5% đuôi trên)",
                         labels={"price_disp": price_axis}),
            use_container_width=True)
        scatter = pdf[(pdf["area_m2"].notna()) & (pdf["area_m2"] > 0) & pdf["price_disp"].notna()]
        if not scatter.empty:
            # cắt outlier CẢ diện tích lẫn giá để biểu đồ không bị 1-2 tin rác làm méo
            scatter = scatter[(scatter["area_m2"] < scatter["area_m2"].quantile(0.95)) &
                              (scatter["price_disp"] < scatter["price_disp"].quantile(0.95))]
        st.plotly_chart(
            px.scatter(scatter, x="area_m2", y="price_disp", color="property_type",
                       title="Giá theo diện tích",
                       labels={"area_m2": "Diện tích (m²)", "price_disp": price_axis,
                               "property_type": "Loại hình"}),
            use_container_width=True)

    with t4:
        geo = pdf.copy()
        has_coords = {"latitude", "longitude"}.issubset(geo.columns)
        if has_coords:
            geo["latitude"] = pd.to_numeric(geo["latitude"], errors="coerce")
            geo["longitude"] = pd.to_numeric(geo["longitude"], errors="coerce")
            # giữ toạ độ hợp lệ trong vùng Việt Nam + có diện tích để vẽ size
            geo = geo[geo["latitude"].between(8, 24)
                      & geo["longitude"].between(102, 110)
                      & geo["area_m2"].notna() & (geo["area_m2"] > 0)]
        if not has_coords or geo.empty:
            st.info("Không có tin nào kèm toạ độ hợp lệ để vẽ bản đồ.")
        else:
            fig = px.scatter_mapbox(
                geo, lat="latitude", lon="longitude",
                color="price_disp", size="area_m2", hover_name="title",
                hover_data={"district": True, "price_disp": ":.2f", "area_m2": ":.0f",
                            "latitude": False, "longitude": False},
                color_continuous_scale="Turbo", size_max=18, zoom=10,
                # cap dải màu ở p95 để 1-2 tin giá rác không nuốt hết màu
                range_color=(geo["price_disp"].min(), geo["price_disp"].quantile(0.95)),
                title=f"Bản đồ tin đăng (màu = giá, {price_unit})",
                labels={"price_disp": price_axis, "area_m2": "DT (m²)", "district": "Khu vực"})
            fig.update_layout(mapbox_style="open-street-map",
                              margin={"l": 0, "r": 0, "t": 40, "b": 0}, height=600)
            st.plotly_chart(fig, use_container_width=True)
            st.caption(f"Hiển thị {len(geo):,} tin có toạ độ.")

    with t5:
        st.dataframe(
            pdf[["title", "property_type", "district", "price_disp", "ppm2",
                 "area_m2", "rooms", "url"]]
            .rename(columns={"title": "Tiêu đề", "property_type": "Loại hình",
                             "district": "Khu vực", "price_disp": f"Giá ({price_unit})",
                             "ppm2": f"Giá/m² ({ppm2_unit})", "area_m2": "DT (m²)",
                             "rooms": "Phòng", "url": "Link"}),
            use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
