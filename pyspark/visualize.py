"""
PySpark Visualization - Xuất biểu đồ bất động sản ra ảnh PNG.
Đọc dữ liệu từ HDFS qua analytics.load_data_from_hdfs().
"""
import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql.functions import col, count, avg

import config
from analytics import create_spark_session, load_data_from_hdfs


def setup_style():
    sns.set_theme(style="darkgrid")
    plt.rcParams["figure.figsize"] = config.CHART_FIGSIZE
    plt.rcParams["figure.dpi"] = config.CHART_DPI


def ensure_output_dir():
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    print(f"Output dir: {config.OUTPUT_DIR}")


def _save(fig, name):
    path = os.path.join(config.OUTPUT_DIR, f"{name}.{config.OUTPUT_FORMAT}")
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> {path}")


def chart_listing_type(df):
    pdf = (df.groupBy("listing_type").agg(count("*").alias("n")).toPandas())
    if pdf.empty:
        return
    fig, ax = plt.subplots(figsize=(6, 6))
    ax.pie(pdf["n"], labels=pdf["listing_type"], autopct="%1.1f%%",
           colors=sns.color_palette("Set2"))
    ax.set_title("Tỷ trọng Bán / Cho thuê", fontweight="bold")
    _save(fig, "listing_type")


def chart_property_type(df):
    pdf = (df.groupBy("property_type").agg(count("*").alias("n"))
           .orderBy(col("n").desc()).limit(config.TOP_N).toPandas())
    fig, ax = plt.subplots()
    sns.barplot(data=pdf, y="property_type", x="n", palette="viridis", ax=ax)
    ax.set_title("Số tin theo loại hình BĐS", fontweight="bold")
    ax.set_xlabel("Số tin"); ax.set_ylabel("")
    _save(fig, "property_type")


def chart_top_districts(df):
    pdf = (df.filter(col("district") != "")
           .groupBy("district").agg(count("*").alias("n"))
           .orderBy(col("n").desc()).limit(config.TOP_N).toPandas())
    fig, ax = plt.subplots()
    sns.barplot(data=pdf, y="district", x="n", palette="mako", ax=ax)
    ax.set_title("Top khu vực theo số tin", fontweight="bold")
    ax.set_xlabel("Số tin"); ax.set_ylabel("")
    _save(fig, "top_districts")


def chart_price_hist(df):
    pdf = (df.filter((col("listing_type") == "Bán") &
                     col("price").isNotNull() & (col("price") > 0))
           .select((col("price") / 1e9).alias("price_ty")).toPandas())
    if pdf.empty:
        return
    pdf = pdf[pdf["price_ty"] < pdf["price_ty"].quantile(0.95)]  # cắt đuôi để dễ nhìn
    fig, ax = plt.subplots()
    sns.histplot(pdf["price_ty"], bins=40, kde=True, color="#2a9d8f", ax=ax)
    ax.set_title("Phân bố giá BÁN (tỷ VND, đã cắt 5% đuôi trên)", fontweight="bold")
    ax.set_xlabel("Giá (tỷ VND)"); ax.set_ylabel("Số tin")
    _save(fig, "price_distribution")


def chart_area_hist(df):
    pdf = (df.filter(col("area_m2").isNotNull() & (col("area_m2") > 0))
           .select("area_m2").toPandas())
    if pdf.empty:
        return
    pdf = pdf[pdf["area_m2"] < pdf["area_m2"].quantile(0.95)]
    fig, ax = plt.subplots()
    sns.histplot(pdf["area_m2"], bins=40, kde=True, color="#e76f51", ax=ax)
    ax.set_title("Phân bố diện tích (m², đã cắt 5% đuôi trên)", fontweight="bold")
    ax.set_xlabel("Diện tích (m²)"); ax.set_ylabel("Số tin")
    _save(fig, "area_distribution")


def chart_avg_price_by_district(df):
    pdf = (df.filter(col("district") != "")
           .filter((col("listing_type") == "Bán") &
                   col("price").isNotNull() & (col("price") > 0))
           .groupBy("district")
           .agg(avg(col("price") / 1e9).alias("avg_price_ty"), count("*").alias("n"))
           .filter(col("n") >= 5)
           .orderBy(col("avg_price_ty").desc()).limit(config.TOP_N).toPandas())
    if pdf.empty:
        return
    fig, ax = plt.subplots()
    sns.barplot(data=pdf, y="district", x="avg_price_ty", palette="rocket", ax=ax)
    ax.set_title("Giá BÁN trung bình theo khu vực (tỷ VND, ≥5 tin)", fontweight="bold")
    ax.set_xlabel("Giá TB (tỷ VND)"); ax.set_ylabel("")
    _save(fig, "avg_price_by_district")


def chart_price_per_m2(df):
    pdf = (df.filter(col("district") != "")
           .filter((col("listing_type") == "Bán") &
                   col("price_per_m2").isNotNull() & (col("price_per_m2") > 0))
           .groupBy("district")
           .agg(avg(col("price_per_m2") / 1e6).alias("ppm2_trieu"), count("*").alias("n"))
           .filter(col("n") >= 5)
           .orderBy(col("ppm2_trieu").desc()).limit(config.TOP_N).toPandas())
    if pdf.empty:
        return
    fig, ax = plt.subplots()
    sns.barplot(data=pdf, y="district", x="ppm2_trieu", palette="flare", ax=ax)
    ax.set_title("Giá BÁN trung bình / m² theo khu vực (triệu VND/m², ≥5 tin)", fontweight="bold")
    ax.set_xlabel("Triệu VND/m²"); ax.set_ylabel("")
    _save(fig, "price_per_m2_by_district")


def main():
    setup_style()
    ensure_output_dir()
    spark = create_spark_session()
    try:
        df = load_data_from_hdfs(spark)
        if df is None:
            return
        df.cache()
        print("Đang tạo biểu đồ...")
        chart_listing_type(df)
        chart_property_type(df)
        chart_top_districts(df)
        chart_price_hist(df)
        chart_area_hist(df)
        chart_avg_price_by_district(df)
        chart_price_per_m2(df)
        print("✅ Hoàn thành.")
    except Exception as e:
        print(f"❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()