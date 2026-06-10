"""
Streamlit Dashboard - Thống kê bất động sản.
Chạy: streamlit run dashboard.py  (cổng 8501)
"""
import pathlib
import sys
import urllib.parse
from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st
from pyarrow import fs

import config

st.set_page_config(page_title="Real Estate Analytics", layout="wide")


# ----------------------------------------------------------------------------- #
# Load data bằng pandas — không cần Spark
# ----------------------------------------------------------------------------- #
@st.cache_data(ttl=300)
def load_data() -> pd.DataFrame | None:
    try:
        # Đường dẫn mạng nội bộ kết nối trực tiếp sang cụm Hadoop NameNode
        webhdfs_url = "webhdfs://namenode:9870/data/real-estate"
        
        # Đọc toàn bộ kho Parquet
        raw_df = pd.read_parquet(webhdfs_url, engine="pyarrow")
        
        if raw_df.empty:
            return None

        # TẠO BẢN SAO SẠCH: Ép toàn bộ DataFrame sao chép ra vùng RAM mới để cắt đứt liên kết Parquet gốc
        df = raw_df.copy()
        
        # TUYỆT CHIÊU PHÁ CATEGORICAL: Chuyển đổi cột thông qua danh sách Python thuần túy
        if "property_type" in df.columns:
            # Bốc giá trị ra thành list chuỗi chữ của Python (list không có khái niệm Categorical)
            pure_strings = [str(x) for x in df["property_type"].tolist()]
            # Đè list sạch này ngược trở lại Dataframe
            df["property_type"] = pure_strings
        
        # Điền các ô trống dữ liệu khuyết
        df["property_type"] = df["property_type"].replace(["nan", "None", "<NA>", ""], "Khác")
        df["property_type"] = df["property_type"].fillna("Khác")
        
        df["district"] = df["district"].astype(str).fillna("").replace(["nan", "None", "<NA>"], "")

        # Xử lý các cột thông số số học phục vụ vẽ biểu đồ Plotly
        df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0)
        df["area_m2"] = pd.to_numeric(df["area_m2"], errors="coerce").fillna(0)
        
        df["price_ty"]            = df["price"] / 1e9
        df["price_per_m2_trieu"]  = (df["price"] / df["area_m2"]).ffill().fillna(0) / 1e6
        
        return df
        
    except Exception as e:
        st.error(f"Lỗi đọc dữ liệu từ HDFS: {e}")
        return None


# ----------------------------------------------------------------------------- #
# Main
# ----------------------------------------------------------------------------- #
def main():
    st.title("🏠 Real Estate Big Data Analytics")
    st.caption(f"Cập nhật: {datetime.now():%Y-%m-%d %H:%M:%S}")

    with st.spinner("Đang tải dữ liệu..."):
        pdf_all = load_data()

    if pdf_all is None or pdf_all.empty:
        st.warning("Chưa có dữ liệu. Hãy chạy crawler → push Kafka → Spark consumer trước.")
        return

    # ---- Sidebar ----
    with st.sidebar:
        st.header("Điều khiển")
        if st.button("🔄 Tải lại dữ liệu", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

        ltype = st.radio("Hình thức", ["Bán", "Cho thuê", "Tất cả"], index=0)

    # ---- Lọc ----
    if ltype != "Tất cả":
        pdf = pdf_all[pdf_all["listing_type"] == ltype].copy()
    else:
        pdf = pdf_all.copy()

    if pdf.empty:
        st.warning(f"Không có tin nào thuộc hình thức: {ltype}")
        return

    st.caption(f"Đang xem: **{ltype}** · {len(pdf):,} tin")

    # ---- Metrics ----
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Tổng số tin",  f"{len(pdf):,}")
    c2.metric("Số khu vực",   f"{pdf['district'].nunique():,}")
    if ltype == "Cho thuê":
        c3.metric("Giá thuê TB", f"{pdf['price'].mean()/1e6:.1f} tr/th")
    else:
        c3.metric("Giá TB", f"{pdf['price_ty'].mean():.2f} tỷ")
    if pdf["area_m2"].notna().any():
        c4.metric("Diện tích TB", f"{pdf['area_m2'].mean():.0f} m²")

    st.markdown("---")

    # ---- Tabs ----
    t1, t2, t3, t4 = st.tabs(
        ["Theo khu vực", "Theo loại hình", "Phân bố giá", "Bảng chi tiết"]
    )

    with t1:
        by_dist = (
            pdf[pdf["district"] != ""]
            .groupby("district")
            .agg(n=("list_id", "count"), avg_price=("price_ty", "mean"))
            .reset_index()
            .sort_values("n", ascending=False)
            .head(config.TOP_N)
        )
        st.plotly_chart(
            px.bar(by_dist, x="n", y="district", orientation="h",
                   title="Số tin theo khu vực",
                   labels={"n": "Số tin", "district": ""}),
            use_container_width=True,
        )
        st.plotly_chart(
            px.bar(by_dist.sort_values("avg_price"), x="avg_price", y="district",
                   orientation="h",
                   title="Giá trung bình theo khu vực (tỷ VND)",
                   labels={"avg_price": "Giá TB (tỷ)", "district": ""}),
            use_container_width=True,
        )

    with t2:
        by_type = (
            pdf.groupby("property_type")
            .size().reset_index(name="n")
            .sort_values("n", ascending=False)
        )
        st.plotly_chart(
            px.pie(by_type, values="n", names="property_type",
                   title="Tỷ trọng theo loại hình"),
            use_container_width=True,
        )

    with t3:
        priced = pdf[pdf["price_ty"].notna() & (pdf["price_ty"] > 0)]
        cap    = priced["price_ty"].quantile(0.95)
        clip   = priced[priced["price_ty"] <= cap]
        st.plotly_chart(
            px.histogram(clip, x="price_ty", nbins=40,
                         title="Phân bố giá (tỷ VND, bỏ 5% đuôi trên)",
                         labels={"price_ty": "Giá (tỷ VND)"}),
            use_container_width=True,
        )
        scatter = pdf[pdf["area_m2"].notna() & (pdf["area_m2"] > 0)].copy()
        area_cap = scatter["area_m2"].quantile(0.95)
        scatter  = scatter[scatter["area_m2"] <= area_cap]
        st.plotly_chart(
            px.scatter(scatter, x="area_m2", y="price_ty", color="property_type",
                       title="Giá theo diện tích",
                       labels={"area_m2": "Diện tích (m²)", "price_ty": "Giá (tỷ)"}),
            use_container_width=True,
        )

    with t4:
        show_cols = ["title", "property_type", "district",
                     "price_ty", "area_m2", "rooms", "url"]
        show_cols = [c for c in show_cols if c in pdf.columns]
        st.dataframe(
            pdf[show_cols].rename(columns={
                "title":         "Tiêu đề",
                "property_type": "Loại hình",
                "district":      "Khu vực",
                "price_ty":      "Giá (tỷ)",
                "area_m2":       "DT (m²)",
                "rooms":         "Phòng",
                "url":           "Link",
            }),
            use_container_width=True,
            hide_index=True,
        )


if __name__ == "__main__":
    main()