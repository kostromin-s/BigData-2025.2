"""
PySpark Visualization - Tạo biểu đồ từ dữ liệu HDFS và xuất ra ảnh
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, length, desc
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend
import seaborn as sns
import pandas as pd
import os
import config
from analytics import create_spark_session, load_data_from_hdfs


def setup_plot_style():
    """Thiết lập style cho biểu đồ"""
    plt.style.use('seaborn-v0_8-darkgrid')
    plt.rcParams['figure.figsize'] = config.CHART_FIGSIZE
    plt.rcParams['figure.dpi'] = config.CHART_DPI
    plt.rcParams['font.size'] = 12
    plt.rcParams['axes.titlesize'] = 14
    plt.rcParams['axes.labelsize'] = 12


def ensure_output_dir():
    """Đảm bảo thư mục output tồn tại"""
    if not os.path.exists(config.OUTPUT_DIR):
        os.makedirs(config.OUTPUT_DIR)
    print(f"Output directory: {config.OUTPUT_DIR}")


def create_domain_bar_chart(df, output_path):
    """Tạo bar chart phân bố theo domain"""
    print("\nDang tao Domain Bar Chart...")
    
    # Lấy thống kê domain
    domain_stats = df.groupBy("domain").agg(
        count("*").alias("doc_count")
    ).orderBy(desc("doc_count")).limit(15)
    
    # Chuyển sang Pandas
    pdf = domain_stats.toPandas()
    
    # Tạo biểu đồ
    fig, ax = plt.subplots(figsize=(14, 8))
    
    colors = sns.color_palette("viridis", len(pdf))
    bars = ax.barh(pdf['domain'], pdf['doc_count'], color=colors)
    
    ax.set_xlabel('Số lượng Documents', fontsize=12)
    ax.set_ylabel('Domain', fontsize=12)
    ax.set_title('Phan Bo Documents Theo Domain', fontsize=16, fontweight='bold')
    
    # Thêm label giá trị
    for bar, val in zip(bars, pdf['doc_count']):
        ax.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height()/2,
                f'{int(val):,}', va='center', fontsize=10)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=config.CHART_DPI, bbox_inches='tight')
    plt.close()
    
    print(f"Đã lưu: {output_path}")


def create_domain_pie_chart(df, output_path):
    """Tạo pie chart phân bố theo domain"""
    print("\nDang tao Domain Pie Chart...")
    
    # Lấy thống kê domain
    domain_stats = df.groupBy("domain").agg(
        count("*").alias("doc_count")
    ).orderBy(desc("doc_count"))
    
    pdf = domain_stats.toPandas()
    
    # Gom các domain nhỏ vào "Others"
    total = pdf['doc_count'].sum()
    threshold = total * 0.02  # Dưới 2% gom vào Others
    
    major = pdf[pdf['doc_count'] >= threshold].copy()
    minor = pdf[pdf['doc_count'] < threshold]
    
    if len(minor) > 0:
        others_row = pd.DataFrame({
            'domain': ['Others'],
            'doc_count': [minor['doc_count'].sum()]
        })
        major = pd.concat([major, others_row], ignore_index=True)
    
    # Tạo biểu đồ 2D (không shadow, không hiệu ứng 3D)
    fig, ax = plt.subplots(figsize=(12, 10))
    
    colors = sns.color_palette("husl", len(major))
    explode = [0.0] * len(major)
    
    wedges, texts, autotexts = ax.pie(
        major['doc_count'],
        labels=major['domain'],
        autopct='%1.1f%%',
        colors=colors,
        explode=explode,
        shadow=False,
        startangle=90
    )
    
    plt.setp(autotexts, size=10, weight='bold')
    ax.set_title('Phan Bo Documents Theo Domain', fontsize=16, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=config.CHART_DPI, bbox_inches='tight')
    plt.close()
    
    print(f"Đã lưu: {output_path}")


def create_content_size_histogram(df, output_path):
    """Tạo histogram phân bố kích thước content"""
    print("\nDang tao Content Size Histogram...")
    
    # Lấy kích thước content
    size_df = df.select(length(col("content")).alias("size"))
    pdf = size_df.toPandas()
    
    # Giới hạn để histogram đẹp hơn (loại bỏ outliers)
    upper_limit = pdf['size'].quantile(0.95)
    filtered_pdf = pdf[pdf['size'] <= upper_limit]
    
    # Tạo biểu đồ
    fig, ax = plt.subplots(figsize=(12, 8))
    
    ax.hist(filtered_pdf['size'], bins=50, color='steelblue', edgecolor='white', alpha=0.7)
    
    ax.set_xlabel('Kích Thước Content (ký tự)', fontsize=12)
    ax.set_ylabel('Số Lượng Documents', fontsize=12)
    ax.set_title('Phan Bo Kich Thuoc Content (den percentile 95)', fontsize=16, fontweight='bold')
    
    # Thêm thông tin thống kê
    stats_text = f"Mean: {pdf['size'].mean():,.0f}\nMedian: {pdf['size'].median():,.0f}\nMax: {pdf['size'].max():,.0f}"
    ax.text(0.95, 0.95, stats_text, transform=ax.transAxes, fontsize=11,
            verticalalignment='top', horizontalalignment='right',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=config.CHART_DPI, bbox_inches='tight')
    plt.close()
    
    print(f"Đã lưu: {output_path}")


def create_size_range_bar_chart(df, output_path):
    """Tạo bar chart phân bố theo khoảng kích thước"""
    print("\nDang tao Size Range Bar Chart...")
    
    size_df = df.select(length(col("content")).alias("size"))
    
    size_ranges = [
        (0, 1000, "0 - 1K"),
        (1000, 5000, "1K - 5K"),
        (5000, 10000, "5K - 10K"),
        (10000, 50000, "10K - 50K"),
        (50000, 100000, "50K - 100K"),
        (100000, float('inf'), "> 100K")
    ]
    
    distribution = []
    for min_s, max_s, label in size_ranges:
        if max_s == float('inf'):
            cnt = size_df.filter(col("size") >= min_s).count()
        else:
            cnt = size_df.filter((col("size") >= min_s) & (col("size") < max_s)).count()
        distribution.append({'range': label, 'count': cnt})
    
    pdf = pd.DataFrame(distribution)
    
    # Tạo biểu đồ
    fig, ax = plt.subplots(figsize=(12, 8))
    
    colors = sns.color_palette("coolwarm", len(pdf))
    bars = ax.bar(pdf['range'], pdf['count'], color=colors, edgecolor='white')
    
    ax.set_xlabel('Khoảng Kích Thước', fontsize=12)
    ax.set_ylabel('Số Lượng Documents', fontsize=12)
    ax.set_title('Phan Bo Documents Theo Khoang Kich Thuoc', fontsize=16, fontweight='bold')
    
    # Thêm label giá trị
    for bar, val in zip(bars, pdf['count']):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                f'{int(val):,}', ha='center', va='bottom', fontsize=11)
    
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(output_path, dpi=config.CHART_DPI, bbox_inches='tight')
    plt.close()
    
    print(f"Đã lưu: {output_path}")


def create_combined_summary(df, output_path):
    """Tạo dashboard tổng hợp"""
    print("\nDang tao Combined Summary Dashboard...")
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 14))
    
    # 1. Domain Distribution (Top 10)
    ax1 = axes[0, 0]
    domain_stats = df.groupBy("domain").agg(count("*").alias("doc_count")) \
        .orderBy(desc("doc_count")).limit(10).toPandas()
    colors = sns.color_palette("viridis", len(domain_stats))
    ax1.barh(domain_stats['domain'], domain_stats['doc_count'], color=colors)
    ax1.set_title('Top 10 Domains', fontweight='bold')
    ax1.set_xlabel('Số lượng Documents')
    
    # 2. Domain Pie Chart (2D)
    ax2 = axes[0, 1]
    colors = sns.color_palette("husl", len(domain_stats))
    ax2.pie(
        domain_stats['doc_count'],
        labels=domain_stats['domain'], 
        autopct='%1.1f%%',
        colors=colors,
        startangle=90,
        shadow=False
    )
    ax2.set_title('Phân Bố Theo Domain', fontweight='bold')
    
    # 3. Content Size Histogram
    ax3 = axes[1, 0]
    size_pdf = df.select(length(col("content")).alias("size")).toPandas()
    upper_limit = size_pdf['size'].quantile(0.95)
    filtered = size_pdf[size_pdf['size'] <= upper_limit]
    ax3.hist(filtered['size'], bins=40, color='steelblue', edgecolor='white', alpha=0.7)
    ax3.set_title('Phân Bố Kích Thước Content', fontweight='bold')
    ax3.set_xlabel('Kích Thước (ký tự)')
    ax3.set_ylabel('Số lượng')
    
    # 4. Summary Statistics
    ax4 = axes[1, 1]
    ax4.axis('off')
    
    total_docs = df.count()
    total_domains = df.select("domain").distinct().count()
    avg_size = size_pdf['size'].mean()
    max_size = size_pdf['size'].max()
    
    summary_text = f"""
    TONG KET THONG KE
    ------------------

    Tong so documents: {total_docs:,}

    So luong domains: {total_domains:,}

    Kich thuoc content:
       - Trung binh: {avg_size:,.0f} ky tu
       - Lon nhat: {max_size:,.0f} ky tu
       
    Tong dung luong: {size_pdf['size'].sum():,.0f} ky tu
    """
    
    ax4.text(0.1, 0.9, summary_text, transform=ax4.transAxes, fontsize=14,
             verticalalignment='top', fontfamily='monospace',
             bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.3))
    
    plt.suptitle('REAL ESTATE DOCUMENTS ANALYTICS DASHBOARD', fontsize=18, fontweight='bold', y=1.02)
    plt.tight_layout()
    plt.savefig(output_path, dpi=config.CHART_DPI, bbox_inches='tight')
    plt.close()
    
    print(f"Đã lưu: {output_path}")


def generate_all_charts(spark):
    """Tạo tất cả các biểu đồ"""
    print("\n" + "=" * 60)
    print("PYSPARK VISUALIZATION - REAL ESTATE DOCUMENTS")
    print("=" * 60)
    
    setup_plot_style()
    ensure_output_dir()
    
    # Load data
    df = load_data_from_hdfs(spark)
    if df is None:
        return
    
    ext = config.OUTPUT_FORMAT
    
    # Generate charts
    create_domain_bar_chart(df, f"{config.OUTPUT_DIR}/domain_bar_chart.{ext}")
    create_domain_pie_chart(df, f"{config.OUTPUT_DIR}/domain_pie_chart.{ext}")
    create_content_size_histogram(df, f"{config.OUTPUT_DIR}/content_size_histogram.{ext}")
    create_size_range_bar_chart(df, f"{config.OUTPUT_DIR}/size_range_chart.{ext}")
    create_combined_summary(df, f"{config.OUTPUT_DIR}/summary_dashboard.{ext}")
    
    print("\n" + "=" * 60)
    print("HOAN THANH TAO BIEU DO")
    print("=" * 60)
    print(f"\nCac file anh da duoc luu tai: {config.OUTPUT_DIR}/")
    print(f"   • domain_bar_chart.{ext}")
    print(f"   • domain_pie_chart.{ext}")
    print(f"   • content_size_histogram.{ext}")
    print(f"   • size_range_chart.{ext}")
    print(f"   • summary_dashboard.{ext}")


def main():
    """Hàm main"""
    spark = create_spark_session()
    
    try:
        generate_all_charts(spark)
    except Exception as e:
        print(f"\n Lỗi: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
