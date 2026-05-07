"""
Big Data Analytics Dashboard - Streamlit Web Interface
Hiển thị thống kê và biểu đồ từ HDFS data
"""
import streamlit as st
import pandas as pd
from pyspark.sql import SparkSession
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import config
import os


# Page config
st.set_page_config(
    page_title="Real Estate Documents Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        padding: 20px 0;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .stProgress > div > div > div > div {
        background-color: #667eea;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_spark_session():
    """Tạo và cache Spark Session"""
    spark = SparkSession.builder \
        .appName("BigData Dashboard") \
        .master(config.SPARK_MASTER) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.hadoop.fs.defaultFS", config.HDFS_NAMENODE) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def load_data(spark):
    """Load data from HDFS"""
    hdfs_path = f"{config.HDFS_NAMENODE}{config.HDFS_INPUT_PATH}"
    try:
        df = spark.read.parquet(hdfs_path)
        return df
    except Exception as e:
        st.error(f"Lỗi khi đọc dữ liệu: {e}")
        return None


def get_pandas_data(df):
    """Convert PySpark DataFrame to Pandas"""
    try:
        # Use collect() instead of toPandas() due to version compatibility
        rows = df.limit(100000).collect()
        
        # Convert to dict
        data = []
        for row in rows:
            data.append(row.asDict())
        
        # Create Pandas DataFrame
        pdf = pd.DataFrame(data)
        return pdf
    except Exception as e:
        st.error(f"Lỗi khi convert dữ liệu: {e}")
        import traceback
        traceback.print_exc()
        return None


def get_overview_stats(df):
    """Lấy thống kê tổng quan"""
    total_docs = len(df)
    avg_size = df['content'].str.len().mean()
    total_size = df['content'].str.len().sum()
    domain_count = df['domain'].nunique()
    
    return {
        "total_docs": total_docs,
        "avg_size": int(avg_size),
        "total_size": total_size,
        "domain_count": domain_count
    }


def create_domain_chart(df):
    """Tạo biểu đồ domain distribution"""
    domain_stats = df.groupby('domain').size().reset_index(name='doc_count')
    domain_stats = domain_stats.nlargest(15, 'doc_count')
    
    fig = px.bar(
        domain_stats,
        y='domain',
        x='doc_count',
        orientation='h',
        title='Top 15 Domains theo số lượng documents',
        labels={'doc_count': 'Số Documents', 'domain': 'Domain'},
        color='doc_count',
        color_continuous_scale='Viridis'
    )
    
    fig.update_layout(
        height=600,
        showlegend=False,
        yaxis={'categoryorder': 'total ascending'}
    )
    
    return fig


def create_size_distribution_chart(df):
    """Tạo biểu đồ phân bố kích thước"""
    sizes = df['content'].str.len()
    
    fig = go.Figure()
    
    fig.add_trace(go.Histogram(
        x=sizes,
        nbinsx=50,
        marker_color='#667eea',
        opacity=0.75,
        name='Documents'
    ))
    
    fig.update_layout(
        title='Phân bố kích thước Documents',
        xaxis_title='Kích thước (ký tự)',
        yaxis_title='Số lượng Documents',
        height=400,
        showlegend=False
    )
    
    return fig


def create_domain_pie_chart(df):
    """Tạo pie chart cho top domains"""
    domain_stats = df.groupby('domain').size().reset_index(name='doc_count')
    domain_stats = domain_stats.nlargest(10, 'doc_count')
    
    fig = px.pie(
        domain_stats,
        values='doc_count',
        names='domain',
        title='Top 10 Domains - Tỷ lệ phân bố',
        hole=0.4,
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    
    fig.update_traces(textposition='inside', textinfo='percent+label')
    fig.update_layout(height=500)
    
    return fig


def create_content_stats_table(df):
    """Tạo bảng thống kê chi tiết"""
    df['content_length'] = df['content'].str.len()
    domain_stats = df.groupby('domain').agg({
        'content': 'count',
        'content_length': ['sum', 'mean']
    }).reset_index()
    
    domain_stats.columns = ['domain', 'doc_count', 'total_chars', 'avg_chars']
    domain_stats = domain_stats.nlargest(20, 'doc_count')
    
    domain_stats['total_chars'] = domain_stats['total_chars'].apply(lambda x: f"{int(x):,}")
    domain_stats['avg_chars'] = domain_stats['avg_chars'].apply(lambda x: f"{int(x):,}")
    domain_stats.columns = ['Domain', 'Số Documents', 'Tổng ký tự', 'TB ký tự']
    
    return domain_stats


def main():
    # Header
    st.markdown('<h1 class="main-header">Real Estate Documents Big Data Analytics</h1>', unsafe_allow_html=True)
    st.markdown("---")
    
    # Sidebar
    with st.sidebar:
        st.image("https://raw.githubusercontent.com/apache/spark/master/docs/img/spark-logo-trademark.png", width=200)
        st.title("Dashboard Controls")
        
        if st.button("Refresh Data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
        
        st.markdown("---")
        st.markdown("### System Info")
        st.info(f"**HDFS:** {config.HDFS_NAMENODE}\n\n**Spark Master:** {config.SPARK_MASTER}")
        
        st.markdown("---")
        st.markdown("### Last Updated")
        st.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    # Load data
    with st.spinner("Đang tải dữ liệu từ HDFS..."):
        spark = get_spark_session()
        df = load_data(spark)
    
    if df is None:
        st.error("Không thể tải dữ liệu từ HDFS. Vui lòng kiểm tra kết nối.")
        return
    
    # Convert to Pandas
    with st.spinner("Đang xử lý dữ liệu..."):
        pdf = get_pandas_data(df)
    
    if pdf is None:
        st.error("Không thể xử lý dữ liệu.")
        return
    
    # Overview metrics
    stats = get_overview_stats(pdf)
    
    st.markdown("## Tổng quan hệ thống")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Tổng Documents",
            value=f"{stats['total_docs']:,}",
            delta="Active"
        )
    
    with col2:
        st.metric(
            label="Số Domains",
            value=f"{stats['domain_count']:,}",
            delta="Categories"
        )
    
    with col3:
        st.metric(
            label="TB Kích thước",
            value=f"{stats['avg_size']:,} chars",
            delta="Per Document"
        )
    
    with col4:
        total_mb = stats['total_size'] / (1024 * 1024)
        st.metric(
            label="Tổng dung lượng",
            value=f"{total_mb:.2f} MB",
            delta="Storage"
        )
    
    st.markdown("---")
    
    # Charts
    tab1, tab2, tab3, tab4 = st.tabs(["Domain Distribution", "Size Analysis", "Domain Pie Chart", "Detailed Table"])
    
    with tab1:
        st.plotly_chart(create_domain_chart(pdf), use_container_width=True)
    
    with tab2:
        st.plotly_chart(create_size_distribution_chart(pdf), use_container_width=True)
        
        # Size ranges analysis
        st.markdown("### Phân bố theo khoảng kích thước")
        size_ranges = [
            (0, 1000, "0 - 1K"),
            (1000, 5000, "1K - 5K"),
            (5000, 10000, "5K - 10K"),
            (10000, 50000, "10K - 50K"),
            (50000, 100000, "50K - 100K"),
            (100000, float('inf'), "> 100K")
        ]
        
        sizes = pdf['content'].str.len()
        range_data = []
        for min_size, max_size, label in size_ranges:
            if max_size == float('inf'):
                count_val = (sizes >= min_size).sum()
            else:
                count_val = ((sizes >= min_size) & (sizes < max_size)).sum()
            range_data.append({"Range": label, "Count": int(count_val)})
        
        range_df = pd.DataFrame(range_data)
        
        col1, col2 = st.columns([2, 1])
        with col1:
            fig_range = px.bar(range_df, x='Range', y='Count', 
                              title='Distribution by Size Range',
                              color='Count',
                              color_continuous_scale='Blues')
            st.plotly_chart(fig_range, use_container_width=True)
        
        with col2:
            st.dataframe(range_df, use_container_width=True, height=400)
    
    with tab3:
        st.plotly_chart(create_domain_pie_chart(pdf), use_container_width=True)
    
    with tab4:
        st.markdown("### Thống kê chi tiết theo Domain")
        table_df = create_content_stats_table(pdf)
        st.dataframe(
            table_df,
            use_container_width=True,
            height=600
        )
        
        # Download button
        csv = table_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name=f'domain_stats_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv',
            mime='text/csv',
        )
    
    # Footer
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center; color: #666;'>
            <p>Powered by Apache Spark & Streamlit | Big Data Analytics Dashboard</p>
        </div>
        """,
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()
