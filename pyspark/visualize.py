"""
Visualization - Xuất biểu đồ BĐS ra PNG.
Đọc Parquet trực tiếp bằng pandas — không cần Spark.
"""
import urllib.parse
import os
import sys
import pathlib
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

import config

OUTPUT_DIR = pathlib.Path(config.OUTPUT_DIR)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
print(f"Output dir: {OUTPUT_DIR}")


def _save(fig, name):
    path = OUTPUT_DIR / f"{name}.{config.OUTPUT_FORMAT}"
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> {path}")


def load_data() -> pd.DataFrame:
    input_path = pathlib.Path(config.HDFS_INPUT_PATH)
    if not input_path.exists():
        print(f"❌ Không tìm thấy {input_path}")
        sys.exit(1)

    files = list(input_path.rglob("*.parquet"))
    if not files:
        print(f"❌ Không có file .parquet trong {input_path}")
        sys.exit(1)

    dfs = []
    for f in files:
        df_part = pd.read_parquet(f)

        # 1. Trường hợp trích xuất từ tên folder phân vùng (Partition)
        if "property_type" not in df_part.columns:
            for part in f.parts:
                if part.startswith("property_type="):
                    raw_val = part.split("=", 1)[1]
                    # GIẢI MÃ %20, %2F THÀNH TIẾNG VIỆT CHUẨN:
                    df_part["property_type"] = urllib.parse.unquote(raw_val)
                    break
        else:
            # 2. Trường hợp cột đã có sẵn trong file nhưng nội dung vẫn bị dính %20
            df_part["property_type"] = df_part["property_type"].apply(
                lambda x: urllib.parse.unquote(str(x)) if pd.notna(x) else x
            )

        dfs.append(df_part)

    df = pd.concat(dfs, ignore_index=True)
    df["price_per_m2"] = df["price"] / df["area_m2"]
    print(f"Đọc được {len(df)} records từ {len(files)} file parquet.")
    return df


def setup_style():
    sns.set_theme(style="darkgrid")
    plt.rcParams["figure.figsize"] = config.CHART_FIGSIZE
    plt.rcParams["figure.dpi"] = config.CHART_DPI

    # THAY ĐỔI: Sử dụng font Arial hoặc Segoe UI để hiển thị tốt tiếng Việt trên Windows
    plt.rcParams["font.family"] = "Segoe UI"  # Hoặc dùng "Arial"
    plt.rcParams["axes.unicode_minus"] = (
        False  # Tránh lỗi hiển thị dấu trừ nếu có số âm
    )


def chart_listing_type(df):
    counts = df["listing_type"].value_counts()
    fig, ax = plt.subplots(figsize=(6, 6))
    ax.pie(counts, labels=counts.index, autopct="%1.1f%%",
           colors=sns.color_palette("Set2"))
    ax.set_title("Tỷ trọng Bán / Cho thuê", fontweight="bold")
    _save(fig, "listing_type")


def chart_property_type(df):
    counts = df["property_type"].value_counts().head(config.TOP_N).reset_index()
    counts.columns = ["property_type", "n"]
    fig, ax = plt.subplots()
    sns.barplot(data=counts, y="property_type", x="n",
                hue="property_type", palette="viridis", legend=False, ax=ax)
    ax.set_title("Số tin theo loại hình BĐS", fontweight="bold")
    ax.set_xlabel("Số tin"); ax.set_ylabel("")
    _save(fig, "property_type")


def chart_top_districts(df):
    counts = (df[df["district"] != ""]["district"]
              .value_counts().head(config.TOP_N).reset_index())
    counts.columns = ["district", "n"]
    fig, ax = plt.subplots()
    sns.barplot(data=counts, y="district", x="n",
                hue="district", palette="mako", legend=False, ax=ax)
    ax.set_title("Top khu vực theo số tin", fontweight="bold")
    ax.set_xlabel("Số tin"); ax.set_ylabel("")
    _save(fig, "top_districts")


def chart_price_hist(df):
    pdf = df[(df["listing_type"] == "Bán") & df["price"].notna() & (df["price"] > 0)].copy()
    if pdf.empty:
        return
    pdf["price_ty"] = pdf["price"] / 1e9
    cap = pdf["price_ty"].quantile(0.95)
    pdf = pdf[pdf["price_ty"] <= cap]
    fig, ax = plt.subplots()
    sns.histplot(pdf["price_ty"], bins=40, kde=True, color="#2a9d8f", ax=ax)
    ax.set_title("Phân bố giá BÁN (tỷ VND, bỏ 5% đuôi trên)", fontweight="bold")
    ax.set_xlabel("Giá (tỷ VND)"); ax.set_ylabel("Số tin")
    _save(fig, "price_distribution")


def chart_area_hist(df):
    pdf = df[df["area_m2"].notna() & (df["area_m2"] > 0)].copy()
    if pdf.empty:
        return
    cap = pdf["area_m2"].quantile(0.95)
    pdf = pdf[pdf["area_m2"] <= cap]
    fig, ax = plt.subplots()
    sns.histplot(pdf["area_m2"], bins=40, kde=True, color="#e76f51", ax=ax)
    ax.set_title("Phân bố diện tích (m², bỏ 5% đuôi trên)", fontweight="bold")
    ax.set_xlabel("Diện tích (m²)"); ax.set_ylabel("Số tin")
    _save(fig, "area_distribution")


def chart_avg_price_by_district(df):
    pdf = df[(df["listing_type"] == "Bán") &
             df["price"].notna() & (df["price"] > 0) &
             df["district"].notna() & (df["district"] != "")].copy()
    pdf["price_ty"] = pdf["price"] / 1e9
    stats = (pdf.groupby("district")
               .agg(avg_price_ty=("price_ty", "mean"), n=("price_ty", "count"))
               .query("n >= 2")
               .sort_values("avg_price_ty", ascending=False)
               .head(config.TOP_N).reset_index())
    if stats.empty:
        return
    fig, ax = plt.subplots()
    sns.barplot(data=stats, y="district", x="avg_price_ty",
                hue="district", palette="rocket", legend=False, ax=ax)
    ax.set_title("Giá BÁN trung bình theo khu vực (tỷ VND)", fontweight="bold")
    ax.set_xlabel("Giá TB (tỷ VND)"); ax.set_ylabel("")
    _save(fig, "avg_price_by_district")


def chart_price_per_m2(df):
    pdf = df[(df["listing_type"] == "Bán") &
             df["price_per_m2"].notna() & (df["price_per_m2"] > 0) &
             df["district"].notna() & (df["district"] != "")].copy()
    pdf["ppm2_trieu"] = pdf["price_per_m2"] / 1e6
    stats = (pdf.groupby("district")
               .agg(ppm2_trieu=("ppm2_trieu", "mean"), n=("ppm2_trieu", "count"))
               .query("n >= 2")
               .sort_values("ppm2_trieu", ascending=False)
               .head(config.TOP_N).reset_index())
    if stats.empty:
        return
    fig, ax = plt.subplots()
    sns.barplot(data=stats, y="district", x="ppm2_trieu",
                hue="district", palette="flare", legend=False, ax=ax)
    ax.set_title("Giá BÁN/m² trung bình theo khu vực (triệu VND/m²)", fontweight="bold")
    ax.set_xlabel("Triệu VND/m²"); ax.set_ylabel("")
    _save(fig, "price_per_m2_by_district")


def main():
    setup_style()
    df = load_data()
    print("Đang tạo biểu đồ...")
    chart_listing_type(df)
    chart_property_type(df)
    chart_top_districts(df)
    chart_price_hist(df)
    chart_area_hist(df)
    chart_avg_price_by_district(df)
    chart_price_per_m2(df)
    print("✅ Hoàn thành.")


if __name__ == "__main__":
    main()