"""
Retail Analytics Dashboard
Cannabis retail sales analytics for Grass Roots and Barbary Coast stores.
Features: S3 data persistence, password authentication, multi-store analysis.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import boto3
from botocore.exceptions import ClientError
import io
import os
from datetime import datetime, timedelta
import hashlib
import json

# =============================================================================
# CONFIGURATION
# =============================================================================

st.set_page_config(
    page_title="Retail Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Store mappings based on data prefixes
STORE_MAPPING = {
    "Barbary Coast - SF Mission": "barbary_coast",
    "Grass Roots - SF": "grass_roots",
}

STORE_DISPLAY_NAMES = {
    "barbary_coast": "Barbary Coast",
    "grass_roots": "Grass Roots"
}

# Sample prefixes to filter out (not actual sales)
# [DS] = Display Samples, [SS] = Staff Samples
SAMPLE_PREFIXES = ["[DS]", "[SS]"]

# =============================================================================
# AUTHENTICATION
# =============================================================================

def check_password():
    """Returns True if the user has entered a correct password."""
    
    def password_entered():
        """Checks whether a password entered by the user is correct."""
        # In production, use environment variables or AWS Secrets Manager
        users = st.secrets.get("passwords", {
            "admin": hashlib.sha256("changeme123".encode()).hexdigest(),
            "analyst": hashlib.sha256("viewonly456".encode()).hexdigest()
        })
        
        entered_hash = hashlib.sha256(st.session_state["password"].encode()).hexdigest()
        
        if st.session_state["username"] in users and users[st.session_state["username"]] == entered_hash:
            st.session_state["password_correct"] = True
            st.session_state["logged_in_user"] = st.session_state["username"]
            del st.session_state["password"]
        else:
            st.session_state["password_correct"] = False

    if st.session_state.get("password_correct", False):
        return True

    # First run or incorrect password
    st.markdown("## 🔐 Login Required")
    st.text_input("Username", key="username")
    st.text_input("Password", type="password", key="password", on_change=password_entered)
    
    if "password_correct" in st.session_state and not st.session_state["password_correct"]:
        st.error("😕 Incorrect username or password")
    
    return False


# =============================================================================
# S3 INTEGRATION
# =============================================================================

class S3DataManager:
    """Manages data persistence with AWS S3."""
    
    def __init__(self):
        self.bucket_name = os.environ.get("S3_BUCKET_NAME", "your-retail-analytics-bucket")
        self.s3_client = None
        self._initialize_client()
    
    def _initialize_client(self):
        """Initialize S3 client with credentials from environment or Streamlit secrets."""
        try:
            # Try environment variables first, then Streamlit secrets
            aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID") or st.secrets.get("aws", {}).get("access_key_id")
            aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY") or st.secrets.get("aws", {}).get("secret_access_key")
            aws_region = os.environ.get("AWS_DEFAULT_REGION", "us-west-2")
            
            if aws_access_key and aws_secret_key:
                self.s3_client = boto3.client(
                    's3',
                    aws_access_key_id=aws_access_key,
                    aws_secret_access_key=aws_secret_key,
                    region_name=aws_region
                )
            else:
                # Try default credentials (IAM role, etc.)
                self.s3_client = boto3.client('s3')
        except Exception as e:
            st.warning(f"S3 connection not configured. Running in local mode. Error: {e}")
            self.s3_client = None
    
    def upload_file(self, file_obj, s3_key: str) -> bool:
        """Upload a file to S3."""
        if not self.s3_client:
            return False
        try:
            self.s3_client.upload_fileobj(file_obj, self.bucket_name, s3_key)
            return True
        except ClientError as e:
            st.error(f"Upload failed: {e}")
            return False
    
    def download_file(self, s3_key: str) -> pd.DataFrame:
        """Download a CSV file from S3 and return as DataFrame."""
        if not self.s3_client:
            return None
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            return pd.read_csv(io.BytesIO(response['Body'].read()))
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                return None
            st.error(f"Download failed: {e}")
            return None
    
    def list_files(self, prefix: str = "") -> list:
        """List files in S3 bucket with given prefix."""
        if not self.s3_client:
            return []
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
            return [obj['Key'] for obj in response.get('Contents', [])]
        except ClientError as e:
            st.error(f"List failed: {e}")
            return []
    
    def save_processed_data(self, df: pd.DataFrame, data_type: str, store: str = "combined"):
        """Save processed data to S3."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        s3_key = f"processed/{store}/{data_type}_{timestamp}.csv"
        
        buffer = io.BytesIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)
        
        return self.upload_file(buffer, s3_key)


# =============================================================================
# DATA PROCESSING
# =============================================================================

class DataProcessor:
    """Processes and cleans uploaded CSV data."""
    
    @staticmethod
    def clean_sales_by_store(df: pd.DataFrame) -> pd.DataFrame:
        """Clean and process Sales by Store data."""
        df = df.copy()
        
        # Convert date columns
        df['Date'] = pd.to_datetime(df['Date'])
        df['Week'] = pd.to_datetime(df['Week'])
        
        # Extract store identifier
        df['Store_ID'] = df['Store'].apply(lambda x: 
            'grass_roots' if 'Grass Roots' in str(x) else 'barbary_coast'
        )
        
        # Ensure numeric columns
        numeric_cols = ['Tickets Count', 'Units Sold', 'Customers Count', 'New Customers',
                       'Gross Sales', 'Discounts', 'Returns', 'Net Sales', 'Taxes',
                       'Gross Receipts', 'COGS (with excise)', 'Gross Income',
                       'Gross Margin %', 'Discount %', 'Cost %',
                       'Avg Basket Size', 'Avg Order Value', 'Avg Order Profit']
        
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df.sort_values('Date')
    
    @staticmethod
    def clean_brand_data(df: pd.DataFrame) -> pd.DataFrame:
        """Clean and process Net Sales by Brand data."""
        df = df.copy()
        
        # Filter out sample records ([DS] = Display Samples, [SS] = Staff Samples)
        # These are not actual sales and should be excluded from analysis
        original_count = len(df)
        df = df[~df['Brand'].str.startswith(('[DS]', '[SS]'), na=False)]
        filtered_count = original_count - len(df)
        
        if filtered_count > 0:
            print(f"Filtered out {filtered_count} sample records ([DS]/[SS])")
        
        # Clean brand name (remove any remaining prefixes like numbers in parentheses)
        df['Brand_Clean'] = df['Brand'].str.strip()
        
        # Ensure numeric columns
        numeric_cols = ['% of Total Net Sales', 'Gross Margin %', 'Avg Cost (w/o excise)', 'Net Sales']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Filter out rows with zero or negative net sales (likely adjustments/corrections)
        df = df[df['Net Sales'] > 0]
        
        return df
    
    @staticmethod
    def clean_product_data(df: pd.DataFrame) -> pd.DataFrame:
        """Clean and process Net Sales by Product data."""
        df = df.copy()
        df['Net Sales'] = pd.to_numeric(df['Net Sales'], errors='coerce')
        return df.sort_values('Net Sales', ascending=False)


# =============================================================================
# ANALYTICS ENGINE
# =============================================================================

class AnalyticsEngine:
    """Generates insights and recommendations from data."""
    
    @staticmethod
    def calculate_store_metrics(df: pd.DataFrame) -> dict:
        """Calculate key metrics by store."""
        metrics = {}
        
        for store_id in df['Store_ID'].unique():
            store_df = df[df['Store_ID'] == store_id]
            store_name = STORE_DISPLAY_NAMES.get(store_id, store_id)
            
            metrics[store_name] = {
                'total_net_sales': store_df['Net Sales'].sum(),
                'total_transactions': store_df['Tickets Count'].sum(),
                'total_customers': store_df['Customers Count'].sum(),
                'total_new_customers': store_df['New Customers'].sum(),
                'avg_order_value': store_df['Avg Order Value'].mean(),
                'avg_margin': store_df['Gross Margin %'].mean() * 100,
                'avg_discount_rate': store_df['Discount %'].mean() * 100,
                'units_sold': store_df['Units Sold'].sum(),
            }
        
        return metrics
    
    @staticmethod
    def identify_top_brands(df: pd.DataFrame, n: int = 10, store: str = None) -> pd.DataFrame:
        """Identify top performing brands."""
        if store and store != 'All Stores':
            store_id = [k for k, v in STORE_DISPLAY_NAMES.items() if v == store]
            if store_id:
                # Check for Store_ID first, then Upload_Store
                if 'Store_ID' in df.columns:
                    df = df[df['Store_ID'] == store_id[0]]
                elif 'Upload_Store' in df.columns:
                    df = df[df['Upload_Store'] == store_id[0]]
        
        return df.nlargest(n, 'Net Sales')[['Brand', 'Net Sales', 'Gross Margin %', '% of Total Net Sales']]
    
    @staticmethod
    def identify_underperformers(df: pd.DataFrame, margin_threshold: float = 0.4) -> pd.DataFrame:
        """Identify brands with low margins that might need attention."""
        low_margin = df[
            (df['Gross Margin %'] < margin_threshold) & 
            (df['Net Sales'] > 1000)  # Only significant sellers
        ].copy()
        
        return low_margin.nsmallest(10, 'Gross Margin %')[['Brand', 'Net Sales', 'Gross Margin %']]
    
    @staticmethod
    def generate_recommendations(store_metrics: dict, brand_df: pd.DataFrame) -> list:
        """Generate actionable business recommendations."""
        recommendations = []
        
        # Compare store performance
        if len(store_metrics) == 2:
            stores = list(store_metrics.keys())
            s1, s2 = stores[0], stores[1]
            
            # AOV comparison
            aov_diff = abs(store_metrics[s1]['avg_order_value'] - store_metrics[s2]['avg_order_value'])
            if aov_diff > 5:
                higher = s1 if store_metrics[s1]['avg_order_value'] > store_metrics[s2]['avg_order_value'] else s2
                lower = s2 if higher == s1 else s1
                recommendations.append({
                    'type': 'opportunity',
                    'title': 'Average Order Value Gap',
                    'description': f"{higher} has ${aov_diff:.2f} higher AOV than {lower}. Consider cross-sell strategies at {lower}.",
                    'priority': 'medium'
                })
            
            # Margin comparison
            margin_diff = abs(store_metrics[s1]['avg_margin'] - store_metrics[s2]['avg_margin'])
            if margin_diff > 3:
                higher = s1 if store_metrics[s1]['avg_margin'] > store_metrics[s2]['avg_margin'] else s2
                lower = s2 if higher == s1 else s1
                recommendations.append({
                    'type': 'warning',
                    'title': 'Margin Disparity',
                    'description': f"{lower} margins are {margin_diff:.1f}% lower than {higher}. Review product mix and pricing.",
                    'priority': 'high'
                })
            
            # New customer comparison
            new_cust_rate_1 = store_metrics[s1]['total_new_customers'] / max(store_metrics[s1]['total_customers'], 1)
            new_cust_rate_2 = store_metrics[s2]['total_new_customers'] / max(store_metrics[s2]['total_customers'], 1)
            if abs(new_cust_rate_1 - new_cust_rate_2) > 0.05:
                higher = s1 if new_cust_rate_1 > new_cust_rate_2 else s2
                recommendations.append({
                    'type': 'insight',
                    'title': 'Customer Acquisition',
                    'description': f"{higher} is attracting more new customers proportionally. Analyze their local marketing.",
                    'priority': 'low'
                })
        
        # Brand recommendations
        if brand_df is not None and len(brand_df) > 0:
            # High margin opportunities
            high_margin_low_sales = brand_df[
                (brand_df['Gross Margin %'] > 0.65) & 
                (brand_df['% of Total Net Sales'] < 0.01)
            ].head(5)
            
            if len(high_margin_low_sales) > 0:
                brands = ", ".join(high_margin_low_sales['Brand'].head(3).tolist())
                recommendations.append({
                    'type': 'opportunity',
                    'title': 'High-Margin Growth Potential',
                    'description': f"Consider promoting {brands} - they have strong margins (>65%) but low sales share.",
                    'priority': 'medium'
                })
        
        return recommendations


# =============================================================================
# VISUALIZATION COMPONENTS
# =============================================================================

def plot_sales_trend(df: pd.DataFrame, store_filter: str = "All Stores"):
    """Create sales trend visualization."""
    if store_filter != "All Stores":
        store_id = [k for k, v in STORE_DISPLAY_NAMES.items() if v == store_filter]
        if store_id:
            df = df[df['Store_ID'] == store_id[0]]
    
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        subplot_titles=('Net Sales by Day', 'Transaction Count'),
                        vertical_spacing=0.1)
    
    for store_id in df['Store_ID'].unique():
        store_df = df[df['Store_ID'] == store_id]
        store_name = STORE_DISPLAY_NAMES.get(store_id, store_id)
        
        fig.add_trace(
            go.Scatter(x=store_df['Date'], y=store_df['Net Sales'],
                      name=f'{store_name} Sales', mode='lines+markers'),
            row=1, col=1
        )
        
        fig.add_trace(
            go.Scatter(x=store_df['Date'], y=store_df['Tickets Count'],
                      name=f'{store_name} Transactions', mode='lines+markers'),
            row=2, col=1
        )
    
    fig.update_layout(height=500, showlegend=True)
    return fig


def plot_category_breakdown(df: pd.DataFrame):
    """Create product category breakdown chart."""
    fig = px.pie(df, values='Net Sales', names='Product Type',
                 title='Sales by Product Category',
                 hole=0.4)
    fig.update_traces(textposition='inside', textinfo='percent+label')
    return fig


def plot_brand_performance(df: pd.DataFrame, top_n: int = 15):
    """Create brand performance visualization."""
    top_brands = df.nlargest(top_n, 'Net Sales')
    
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    fig.add_trace(
        go.Bar(x=top_brands['Brand'], y=top_brands['Net Sales'],
               name='Net Sales', marker_color='steelblue'),
        secondary_y=False
    )
    
    fig.add_trace(
        go.Scatter(x=top_brands['Brand'], y=top_brands['Gross Margin %'] * 100,
                   name='Gross Margin %', mode='lines+markers',
                   marker_color='coral', line=dict(width=2)),
        secondary_y=True
    )
    
    fig.update_layout(
        title=f'Top {top_n} Brands by Net Sales with Margin Overlay',
        xaxis_tickangle=-45,
        height=500
    )
    fig.update_yaxes(title_text="Net Sales ($)", secondary_y=False)
    fig.update_yaxes(title_text="Gross Margin (%)", secondary_y=True)
    
    return fig


def plot_store_comparison(metrics: dict):
    """Create store comparison dashboard."""
    stores = list(metrics.keys())
    
    comparison_data = {
        'Metric': ['Net Sales', 'Transactions', 'Avg Order Value', 'Gross Margin %', 'Units Sold'],
    }
    
    for store in stores:
        comparison_data[store] = [
            metrics[store]['total_net_sales'],
            metrics[store]['total_transactions'],
            metrics[store]['avg_order_value'],
            metrics[store]['avg_margin'],
            metrics[store]['units_sold']
        ]
    
    fig = go.Figure()
    
    # Normalized comparison
    for store in stores:
        # Normalize for radar chart
        values = comparison_data[store]
        max_vals = [max(comparison_data[s][i] for s in stores) for i in range(len(values))]
        normalized = [v / m * 100 if m > 0 else 0 for v, m in zip(values, max_vals)]
        normalized.append(normalized[0])  # Close the polygon
        
        categories = comparison_data['Metric'] + [comparison_data['Metric'][0]]
        
        fig.add_trace(go.Scatterpolar(
            r=normalized,
            theta=categories,
            fill='toself',
            name=store
        ))
    
    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
        showlegend=True,
        title='Store Performance Comparison (Normalized)'
    )
    
    return fig


# =============================================================================
# MAIN APPLICATION
# =============================================================================

def main():
    """Main application entry point."""
    
    # Authentication check
    if not check_password():
        st.stop()
    
    # Initialize services
    s3_manager = S3DataManager()
    processor = DataProcessor()
    analytics = AnalyticsEngine()
    
    # Initialize session state for data
    if 'sales_data' not in st.session_state:
        st.session_state.sales_data = None
    if 'brand_data' not in st.session_state:
        st.session_state.brand_data = None
    if 'product_data' not in st.session_state:
        st.session_state.product_data = None
    
    # Sidebar
    with st.sidebar:
        st.image("https://via.placeholder.com/150x50?text=Your+Logo", width=150)
        st.markdown(f"**Logged in as:** {st.session_state.get('logged_in_user', 'Unknown')}")
        st.markdown("---")
        
        # Navigation
        page = st.radio("Navigation", [
            "📊 Dashboard",
            "📈 Sales Analysis", 
            "🏷️ Brand Performance",
            "📦 Product Categories",
            "💡 Recommendations",
            "📤 Data Upload"
        ])
        
        st.markdown("---")
        
        # Store filter (global)
        store_options = ["All Stores", "Barbary Coast", "Grass Roots"]
        selected_store = st.selectbox("Filter by Store", store_options)
        
        # Date range filter
        st.markdown("**Date Range**")
        date_range = st.date_input(
            "Select dates",
            value=(datetime.now() - timedelta(days=30), datetime.now()),
            key="date_filter"
        )
        
        if st.button("🚪 Logout"):
            st.session_state.clear()
            st.rerun()
    
    # Main content area
    st.title("🌿 Retail Analytics Dashboard")
    
    # Page routing
    if page == "📊 Dashboard":
        render_dashboard(st.session_state, analytics, selected_store)
    
    elif page == "📈 Sales Analysis":
        render_sales_analysis(st.session_state, selected_store)
    
    elif page == "🏷️ Brand Performance":
        render_brand_analysis(st.session_state, analytics, selected_store)
    
    elif page == "📦 Product Categories":
        render_product_analysis(st.session_state)
    
    elif page == "💡 Recommendations":
        render_recommendations(st.session_state, analytics)
    
    elif page == "📤 Data Upload":
        render_upload_page(s3_manager, processor)


def render_dashboard(state, analytics, store_filter):
    """Render main dashboard overview."""
    st.header("Overview Dashboard")
    
    if state.sales_data is None:
        st.info("👆 Upload your data files using the 'Data Upload' page to get started.")
        
        # Show demo data option
        if st.button("Load Demo Data"):
            st.info("In production, this would load sample data for demonstration.")
        return
    
    # Calculate metrics
    metrics = analytics.calculate_store_metrics(state.sales_data)
    
    # KPI Cards
    col1, col2, col3, col4 = st.columns(4)
    
    total_sales = sum(m['total_net_sales'] for m in metrics.values())
    total_transactions = sum(m['total_transactions'] for m in metrics.values())
    avg_aov = sum(m['avg_order_value'] for m in metrics.values()) / len(metrics)
    avg_margin = sum(m['avg_margin'] for m in metrics.values()) / len(metrics)
    
    with col1:
        st.metric("Total Net Sales", f"${total_sales:,.0f}")
    with col2:
        st.metric("Total Transactions", f"{total_transactions:,}")
    with col3:
        st.metric("Avg Order Value", f"${avg_aov:.2f}")
    with col4:
        st.metric("Avg Gross Margin", f"{avg_margin:.1f}%")
    
    st.markdown("---")
    
    # Charts row
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Sales Trend")
        fig = plot_sales_trend(state.sales_data, store_filter)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Store Comparison")
        fig = plot_store_comparison(metrics)
        st.plotly_chart(fig, use_container_width=True)
    
    # Product breakdown
    if state.product_data is not None:
        st.subheader("Product Category Mix")
        fig = plot_category_breakdown(state.product_data)
        st.plotly_chart(fig, use_container_width=True)


def render_sales_analysis(state, store_filter):
    """Render detailed sales analysis page."""
    st.header("Sales Analysis")
    
    if state.sales_data is None:
        st.warning("Please upload sales data first.")
        return
    
    df = state.sales_data.copy()
    
    # Apply store filter
    if store_filter != "All Stores":
        store_id = [k for k, v in STORE_DISPLAY_NAMES.items() if v == store_filter]
        if store_id:
            df = df[df['Store_ID'] == store_id[0]]
    
    # Tabs for different views
    tab1, tab2, tab3 = st.tabs(["📈 Trends", "📊 Daily Breakdown", "🔍 Raw Data"])
    
    with tab1:
        col1, col2 = st.columns(2)
        
        with col1:
            # Sales trend
            fig = px.line(df, x='Date', y='Net Sales', color='Store_ID',
                         title='Daily Net Sales Trend')
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Customer trend
            fig = px.line(df, x='Date', y='Customers Count', color='Store_ID',
                         title='Daily Customer Count')
            st.plotly_chart(fig, use_container_width=True)
        
        # Margin trend
        fig = px.line(df, x='Date', y='Gross Margin %', color='Store_ID',
                     title='Gross Margin % Trend')
        st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        # Day of week analysis
        df['Day_of_Week'] = df['Date'].dt.day_name()
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        
        dow_sales = df.groupby(['Day_of_Week', 'Store_ID'])['Net Sales'].mean().reset_index()
        dow_sales['Day_of_Week'] = pd.Categorical(dow_sales['Day_of_Week'], categories=day_order, ordered=True)
        dow_sales = dow_sales.sort_values('Day_of_Week')
        
        fig = px.bar(dow_sales, x='Day_of_Week', y='Net Sales', color='Store_ID',
                    barmode='group', title='Average Sales by Day of Week')
        st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        st.dataframe(df.sort_values('Date', ascending=False), use_container_width=True)
        
        # Download button
        csv = df.to_csv(index=False)
        st.download_button("📥 Download Data", csv, "sales_data.csv", "text/csv")


def render_brand_analysis(state, analytics, store_filter):
    """Render brand performance analysis page."""
    st.header("Brand Performance Analysis")
    
    if state.brand_data is None:
        st.warning("Please upload brand data first.")
        return
    
    df = state.brand_data.copy()
    
    # Top performers
    st.subheader("Top Performing Brands")
    top_n = st.slider("Number of brands to show", 10, 50, 20)
    
    fig = plot_brand_performance(df, top_n)
    st.plotly_chart(fig, use_container_width=True)
    
    # Brand table
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🏆 Top Brands by Revenue")
        top_brands = analytics.identify_top_brands(df, 10, store_filter)
        st.dataframe(top_brands, use_container_width=True)
    
    with col2:
        st.subheader("⚠️ Low Margin Brands")
        underperformers = analytics.identify_underperformers(df)
        st.dataframe(underperformers, use_container_width=True)
    
    # Margin vs Sales scatter
    st.subheader("Margin vs. Sales Analysis")
    
    # Filter to significant brands
    significant_brands = df[df['Net Sales'] > 10000].copy()
    
    fig = px.scatter(significant_brands, 
                    x='Net Sales', 
                    y='Gross Margin %',
                    hover_data=['Brand'],
                    title='Brand Positioning: Sales vs Margin',
                    log_x=True)
    
    # Add quadrant lines
    fig.add_hline(y=0.55, line_dash="dash", line_color="gray", annotation_text="Target Margin")
    
    st.plotly_chart(fig, use_container_width=True)


def render_product_analysis(state):
    """Render product category analysis page."""
    st.header("Product Category Analysis")
    
    if state.product_data is None:
        st.warning("Please upload product data first.")
        return
    
    df = state.product_data.copy()
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = plot_category_breakdown(df)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Category Details")
        df['Sales Share %'] = (df['Net Sales'] / df['Net Sales'].sum() * 100).round(2)
        st.dataframe(df, use_container_width=True)
    
    # Category bar chart
    fig = px.bar(df, x='Product Type', y='Net Sales',
                title='Net Sales by Product Category',
                color='Net Sales',
                color_continuous_scale='Blues')
    st.plotly_chart(fig, use_container_width=True)


def render_recommendations(state, analytics):
    """Render AI-powered recommendations page."""
    st.header("💡 Business Recommendations")
    
    if state.sales_data is None:
        st.warning("Please upload data to generate recommendations.")
        return
    
    # Generate recommendations
    metrics = analytics.calculate_store_metrics(state.sales_data)
    recommendations = analytics.generate_recommendations(metrics, state.brand_data)
    
    if not recommendations:
        st.info("No specific recommendations at this time. Data looks good!")
        return
    
    # Display recommendations by priority
    st.subheader("🔴 High Priority")
    high_priority = [r for r in recommendations if r['priority'] == 'high']
    for rec in high_priority:
        with st.expander(f"⚠️ {rec['title']}", expanded=True):
            st.write(rec['description'])
    
    st.subheader("🟡 Medium Priority")
    medium_priority = [r for r in recommendations if r['priority'] == 'medium']
    for rec in medium_priority:
        with st.expander(f"📊 {rec['title']}"):
            st.write(rec['description'])
    
    st.subheader("🟢 Insights")
    low_priority = [r for r in recommendations if r['priority'] == 'low']
    for rec in low_priority:
        with st.expander(f"💡 {rec['title']}"):
            st.write(rec['description'])


def render_upload_page(s3_manager, processor):
    """Render data upload page."""
    st.header("📤 Data Upload")
    
    st.markdown("""
    Upload your CSV files to update the dashboard. Supported file types:
    - **Sales by Store**: Daily transaction and revenue data
    - **Net Sales by Brand**: Brand-level performance metrics
    - **Net Sales by Product**: Product category summaries
    """)
    
    # Global upload settings
    st.markdown("---")
    st.subheader("📋 Upload Settings")
    
    settings_col1, settings_col2 = st.columns(2)
    
    with settings_col1:
        selected_store = st.selectbox(
            "Select Store",
            options=["Barbary Coast", "Grass Roots", "Both Stores (Combined)"],
            help="Which store does this data belong to?"
        )
        
        # Map display name to internal ID
        store_id_map = {
            "Barbary Coast": "barbary_coast",
            "Grass Roots": "grass_roots",
            "Both Stores (Combined)": "combined"
        }
        store_id = store_id_map[selected_store]
    
    with settings_col2:
        date_range = st.date_input(
            "Data Date Range",
            value=(datetime.now() - timedelta(days=30), datetime.now()),
            help="What date range does this data cover?"
        )
        
        # Handle single date vs range
        if isinstance(date_range, tuple) and len(date_range) == 2:
            start_date, end_date = date_range
        else:
            start_date = end_date = date_range
    
    # Display selected settings
    st.info(f"📍 **Store:** {selected_store} | 📅 **Period:** {start_date.strftime('%b %d, %Y')} to {end_date.strftime('%b %d, %Y')}")
    
    st.markdown("---")
    
    # File uploaders
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.subheader("Sales Data")
        sales_file = st.file_uploader("Upload Sales by Store CSV", type=['csv'], key='sales_upload')
        
        if sales_file:
            df = pd.read_csv(sales_file)
            st.success(f"Loaded {len(df)} rows")
            
            # Preview
            with st.expander("Preview Data"):
                st.dataframe(df.head(), use_container_width=True)
            
            if st.button("Process Sales Data", key="process_sales"):
                processed = processor.clean_sales_by_store(df)
                
                # Add metadata
                processed['Upload_Store'] = store_id
                processed['Upload_Start_Date'] = pd.to_datetime(start_date)
                processed['Upload_End_Date'] = pd.to_datetime(end_date)
                
                # If store is manually specified and not "combined", override Store_ID
                if store_id != "combined":
                    processed['Store_ID'] = store_id
                
                # Merge with existing data or replace
                if st.session_state.sales_data is not None:
                    # Option to append or replace
                    st.session_state.sales_data = pd.concat([
                        st.session_state.sales_data,
                        processed
                    ]).drop_duplicates(subset=['Store', 'Date'], keep='last')
                else:
                    st.session_state.sales_data = processed
                
                # Upload to S3 with metadata in path
                sales_file.seek(0)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                date_range_str = f"{start_date.strftime('%Y%m%d')}-{end_date.strftime('%Y%m%d')}"
                s3_key = f"raw-uploads/{store_id}/sales_{date_range_str}_{timestamp}.csv"
                
                if s3_manager.upload_file(sales_file, s3_key):
                    st.success(f"✅ Uploaded to S3: {s3_key}")
                else:
                    st.info("Running locally (S3 not configured)")
                
                st.success("✅ Data processed and ready!")
                st.rerun()
    
    with col2:
        st.subheader("Brand Data")
        brand_file = st.file_uploader("Upload Net Sales by Brand CSV", type=['csv'], key='brand_upload')
        
        if brand_file:
            df = pd.read_csv(brand_file)
            st.success(f"Loaded {len(df)} rows")
            
            # Show sample record count that will be filtered
            sample_count = df['Brand'].str.startswith(('[DS]', '[SS]'), na=False).sum()
            if sample_count > 0:
                st.info(f"ℹ️ {sample_count} sample records ([DS]/[SS]) will be filtered out")
            
            # Preview
            with st.expander("Preview Data"):
                st.dataframe(df.head(), use_container_width=True)
            
            if st.button("Process Brand Data", key="process_brand"):
                original_count = len(df)
                processed = processor.clean_brand_data(df)
                filtered_count = original_count - len(processed)
                
                if filtered_count > 0:
                    st.info(f"Filtered out {filtered_count} records (samples + zero/negative sales)")
                
                # Add metadata
                processed['Upload_Store'] = store_id
                processed['Upload_Start_Date'] = pd.to_datetime(start_date)
                processed['Upload_End_Date'] = pd.to_datetime(end_date)
                
                # If store is manually specified and not "combined", set Store_ID
                if store_id != "combined":
                    processed['Store_ID'] = store_id
                
                # Merge with existing data or replace
                if st.session_state.brand_data is not None:
                    st.session_state.brand_data = pd.concat([
                        st.session_state.brand_data,
                        processed
                    ]).drop_duplicates(subset=['Brand', 'Upload_Store', 'Upload_Start_Date'], keep='last')
                else:
                    st.session_state.brand_data = processed
                
                brand_file.seek(0)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                date_range_str = f"{start_date.strftime('%Y%m%d')}-{end_date.strftime('%Y%m%d')}"
                s3_key = f"raw-uploads/{store_id}/brand_{date_range_str}_{timestamp}.csv"
                
                if s3_manager.upload_file(brand_file, s3_key):
                    st.success(f"✅ Uploaded to S3: {s3_key}")
                else:
                    st.info("Running locally (S3 not configured)")
                
                st.success("✅ Data processed!")
                st.rerun()
    
    with col3:
        st.subheader("Product Data")
        product_file = st.file_uploader("Upload Net Sales by Product CSV", type=['csv'], key='product_upload')
        
        if product_file:
            df = pd.read_csv(product_file)
            st.success(f"Loaded {len(df)} rows")
            
            # Preview
            with st.expander("Preview Data"):
                st.dataframe(df.head(), use_container_width=True)
            
            if st.button("Process Product Data", key="process_product"):
                processed = processor.clean_product_data(df)
                
                # Add metadata
                processed['Upload_Store'] = store_id
                processed['Upload_Start_Date'] = pd.to_datetime(start_date)
                processed['Upload_End_Date'] = pd.to_datetime(end_date)
                
                # Merge with existing data or replace
                if st.session_state.product_data is not None:
                    st.session_state.product_data = pd.concat([
                        st.session_state.product_data,
                        processed
                    ]).drop_duplicates(subset=['Product Type', 'Upload_Store', 'Upload_Start_Date'], keep='last')
                else:
                    st.session_state.product_data = processed
                
                product_file.seek(0)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                date_range_str = f"{start_date.strftime('%Y%m%d')}-{end_date.strftime('%Y%m%d')}"
                s3_key = f"raw-uploads/{store_id}/product_{date_range_str}_{timestamp}.csv"
                
                if s3_manager.upload_file(product_file, s3_key):
                    st.success(f"✅ Uploaded to S3: {s3_key}")
                else:
                    st.info("Running locally (S3 not configured)")
                
                st.success("✅ Data processed!")
                st.rerun()
    
    # Current data status
    st.markdown("---")
    st.subheader("📊 Current Data Status")
    
    status_col1, status_col2, status_col3 = st.columns(3)
    
    with status_col1:
        if st.session_state.sales_data is not None:
            df = st.session_state.sales_data
            stores = df['Store_ID'].unique().tolist() if 'Store_ID' in df.columns else ['Unknown']
            st.success(f"✅ Sales: {len(df)} records")
            st.caption(f"Stores: {', '.join(stores)}")
            if 'Date' in df.columns:
                st.caption(f"Date range: {df['Date'].min().strftime('%Y-%m-%d')} to {df['Date'].max().strftime('%Y-%m-%d')}")
        else:
            st.warning("❌ No sales data loaded")
    
    with status_col2:
        if st.session_state.brand_data is not None:
            df = st.session_state.brand_data
            stores = df['Upload_Store'].unique().tolist() if 'Upload_Store' in df.columns else ['Unknown']
            st.success(f"✅ Brands: {len(df)} records")
            st.caption(f"Stores: {', '.join(stores)}")
        else:
            st.warning("❌ No brand data loaded")
    
    with status_col3:
        if st.session_state.product_data is not None:
            df = st.session_state.product_data
            stores = df['Upload_Store'].unique().tolist() if 'Upload_Store' in df.columns else ['Unknown']
            st.success(f"✅ Products: {len(df)} records")
            st.caption(f"Stores: {', '.join(stores)}")
        else:
            st.warning("❌ No product data loaded")
    
    # Data management section
    st.markdown("---")
    st.subheader("🗂️ Data Management")
    
    mgmt_col1, mgmt_col2 = st.columns(2)
    
    with mgmt_col1:
        if st.button("🗑️ Clear All Data", type="secondary"):
            st.session_state.sales_data = None
            st.session_state.brand_data = None
            st.session_state.product_data = None
            st.success("All data cleared!")
            st.rerun()
    
    with mgmt_col2:
        if st.session_state.sales_data is not None or st.session_state.brand_data is not None:
            if st.button("📥 Export Combined Data"):
                # Create a summary export
                export_data = {
                    'export_date': datetime.now().isoformat(),
                    'sales_records': len(st.session_state.sales_data) if st.session_state.sales_data is not None else 0,
                    'brand_records': len(st.session_state.brand_data) if st.session_state.brand_data is not None else 0,
                    'product_records': len(st.session_state.product_data) if st.session_state.product_data is not None else 0,
                }
                st.json(export_data)


if __name__ == "__main__":
    main()
