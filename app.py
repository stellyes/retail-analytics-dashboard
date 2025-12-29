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

# Import Claude AI integration (optional)
try:
    from claude_integration import ClaudeAnalytics
    CLAUDE_AVAILABLE = True
except ImportError:
    CLAUDE_AVAILABLE = False

# Import Research Integration (optional)
try:
    from research_integration import render_research_page
    RESEARCH_AVAILABLE = True
except ImportError:
    RESEARCH_AVAILABLE = False

# Import SEO Integration (optional)
try:
    from seo_integration import render_seo_page
    SEO_AVAILABLE = True
except ImportError:
    SEO_AVAILABLE = False

# Import Manual Research Integration (optional)
try:
    from manual_research_integration import render_manual_research_page
    MANUAL_RESEARCH_AVAILABLE = True
except ImportError:
    MANUAL_RESEARCH_AVAILABLE = False

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
        self.bucket_name = None
        self.s3_client = None
        self.connection_error = None
        self._initialize_client()
    
    def _initialize_client(self):
        """Initialize S3 client with credentials from environment or Streamlit secrets."""
        try:
            # Try environment variables first, then Streamlit secrets
            aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
            aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
            aws_region = os.environ.get("AWS_DEFAULT_REGION", "us-west-2")
            bucket_name = os.environ.get("S3_BUCKET_NAME")
            
            # Fall back to Streamlit secrets
            if not aws_access_key:
                try:
                    aws_secrets = st.secrets.get("aws", {})
                    aws_access_key = aws_secrets.get("access_key_id")
                    aws_secret_key = aws_secrets.get("secret_access_key")
                    aws_region = aws_secrets.get("region", "us-west-2")
                    bucket_name = aws_secrets.get("bucket_name")
                except Exception as e:
                    self.connection_error = f"Could not read secrets: {e}"
                    return
            
            if not bucket_name:
                self.connection_error = "No bucket_name configured in secrets.toml [aws] section"
                return
            
            self.bucket_name = bucket_name
            
            if aws_access_key and aws_secret_key:
                self.s3_client = boto3.client(
                    's3',
                    aws_access_key_id=aws_access_key,
                    aws_secret_access_key=aws_secret_key,
                    region_name=aws_region
                )
            else:
                self.connection_error = "Missing AWS credentials (access_key_id or secret_access_key)"
                return
                
        except Exception as e:
            self.connection_error = f"S3 initialization error: {e}"
            self.s3_client = None
    
    def is_configured(self) -> bool:
        """Check if S3 is properly configured."""
        return self.s3_client is not None and self.bucket_name is not None
    
    def test_connection(self) -> tuple[bool, str]:
        """Test S3 connection by attempting to list bucket contents."""
        if not self.is_configured():
            return False, self.connection_error or "S3 not configured"
        
        try:
            # Try to list objects (even empty bucket should work)
            self.s3_client.list_objects_v2(Bucket=self.bucket_name, MaxKeys=1)
            return True, f"Connected to bucket: {self.bucket_name}"
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            error_msg = e.response.get('Error', {}).get('Message', str(e))
            return False, f"S3 Error ({error_code}): {error_msg}"
        except Exception as e:
            return False, f"Connection test failed: {e}"
    
    def upload_file(self, file_obj, s3_key: str) -> tuple[bool, str]:
        """Upload a file to S3. Returns (success, message)."""
        if not self.is_configured():
            return False, self.connection_error or "S3 not configured"
        
        try:
            self.s3_client.upload_fileobj(file_obj, self.bucket_name, s3_key)
            return True, f"Uploaded to s3://{self.bucket_name}/{s3_key}"
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            error_msg = e.response.get('Error', {}).get('Message', str(e))
            return False, f"Upload failed ({error_code}): {error_msg}"
        except Exception as e:
            return False, f"Upload failed: {e}"
    
    def download_file(self, s3_key: str) -> pd.DataFrame:
        """Download a CSV file from S3 and return as DataFrame."""
        if not self.is_configured():
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
        if not self.is_configured():
            return []
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
            return [obj['Key'] for obj in response.get('Contents', [])]
        except ClientError as e:
            st.error(f"List failed: {e}")
            return []
    
    def save_processed_data(self, df: pd.DataFrame, data_type: str, store: str = "combined") -> tuple[bool, str]:
        """Save processed data to S3."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        s3_key = f"processed/{store}/{data_type}_{timestamp}.csv"
        
        buffer = io.BytesIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)
        
        return self.upload_file(buffer, s3_key)
    
    def save_brand_product_mapping(self, mapping: dict) -> tuple[bool, str]:
        """Save brand-product mapping to S3."""
        if not self.is_configured():
            return False, self.connection_error or "S3 not configured"
        
        s3_key = "config/brand_product_mapping.json"
        
        try:
            mapping_json = json.dumps(mapping, indent=2)
            buffer = io.BytesIO(mapping_json.encode('utf-8'))
            self.s3_client.upload_fileobj(buffer, self.bucket_name, s3_key)
            return True, f"Saved mapping to s3://{self.bucket_name}/{s3_key}"
        except ClientError as e:
            error_msg = e.response.get('Error', {}).get('Message', str(e))
            return False, f"Failed to save mapping: {error_msg}"
        except Exception as e:
            return False, f"Failed to save mapping: {e}"
    
    def load_brand_product_mapping(self) -> dict:
        """Load brand-product mapping from S3."""
        if not self.is_configured():
            return {}
        
        s3_key = "config/brand_product_mapping.json"
        
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            mapping_json = response['Body'].read().decode('utf-8')
            return json.loads(mapping_json)
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                # No mapping file exists yet - that's OK
                return {}
            return {}
        except Exception:
            return {}
    
    def load_all_data_from_s3(self, processor) -> dict:
        """
        Load all uploaded data from S3 and return merged DataFrames.
        
        Args:
            processor: DataProcessor instance for cleaning data
            
        Returns:
            Dict with 'sales', 'brand', 'product' DataFrames (or None if no data)
        """
        if not self.is_configured():
            return {'sales': None, 'brand': None, 'product': None}
        
        result = {'sales': None, 'brand': None, 'product': None}
        
        try:
            # List all files in raw-uploads
            files = self.list_files(prefix="raw-uploads/")
            
            if not files:
                return result
            
            # Group files by type
            sales_files = [f for f in files if '/sales_' in f and f.endswith('.csv')]
            brand_files = [f for f in files if '/brand_' in f and f.endswith('.csv')]
            product_files = [f for f in files if '/product_' in f and f.endswith('.csv')]
            
            # Load and merge sales data
            if sales_files:
                sales_dfs = []
                for f in sales_files:
                    try:
                        df = self.download_file(f)
                        if df is not None and not df.empty:
                            # Extract store from path
                            store_id = self._extract_store_from_path(f)
                            df = processor.clean_sales_by_store(df)
                            if store_id and store_id != 'combined':
                                df['Upload_Store'] = store_id
                            sales_dfs.append(df)
                    except Exception as e:
                        print(f"Error loading {f}: {e}")
                        continue
                
                if sales_dfs:
                    result['sales'] = pd.concat(sales_dfs, ignore_index=True)
                    # Remove duplicates based on Store and Date
                    result['sales'] = result['sales'].drop_duplicates(
                        subset=['Store', 'Date'], 
                        keep='last'
                    )
            
            # Load and merge brand data
            if brand_files:
                brand_dfs = []
                for f in brand_files:
                    try:
                        df = self.download_file(f)
                        if df is not None and not df.empty:
                            store_id = self._extract_store_from_path(f)
                            date_range = self._extract_date_range_from_path(f)
                            
                            df = processor.clean_brand_data(df)
                            df['Upload_Store'] = store_id
                            
                            if date_range:
                                df['Upload_Start_Date'] = pd.to_datetime(date_range[0])
                                df['Upload_End_Date'] = pd.to_datetime(date_range[1])
                            
                            brand_dfs.append(df)
                    except Exception as e:
                        print(f"Error loading {f}: {e}")
                        continue
                
                if brand_dfs:
                    result['brand'] = pd.concat(brand_dfs, ignore_index=True)
                    # Remove duplicates
                    if 'Upload_Start_Date' in result['brand'].columns:
                        result['brand'] = result['brand'].drop_duplicates(
                            subset=['Brand', 'Upload_Store', 'Upload_Start_Date'], 
                            keep='last'
                        )
            
            # Load and merge product data
            if product_files:
                product_dfs = []
                for f in product_files:
                    try:
                        df = self.download_file(f)
                        if df is not None and not df.empty:
                            store_id = self._extract_store_from_path(f)
                            date_range = self._extract_date_range_from_path(f)
                            
                            df = processor.clean_product_data(df)
                            df['Upload_Store'] = store_id
                            
                            if date_range:
                                df['Upload_Start_Date'] = pd.to_datetime(date_range[0])
                                df['Upload_End_Date'] = pd.to_datetime(date_range[1])
                            
                            product_dfs.append(df)
                    except Exception as e:
                        print(f"Error loading {f}: {e}")
                        continue
                
                if product_dfs:
                    result['product'] = pd.concat(product_dfs, ignore_index=True)
                    # Remove duplicates
                    if 'Upload_Start_Date' in result['product'].columns:
                        result['product'] = result['product'].drop_duplicates(
                            subset=['Product Type', 'Upload_Store', 'Upload_Start_Date'], 
                            keep='last'
                        )
        
        except Exception as e:
            print(f"Error loading data from S3: {e}")
        
        return result
    
    def _extract_store_from_path(self, path: str) -> str:
        """Extract store ID from S3 file path."""
        # Path format: raw-uploads/{store_id}/type_daterange_timestamp.csv
        parts = path.split('/')
        if len(parts) >= 2:
            return parts[1]  # e.g., 'barbary_coast', 'grass_roots', 'combined'
        return 'combined'
    
    def _extract_date_range_from_path(self, path: str) -> tuple:
        """Extract date range from S3 file path."""
        # Filename format: type_YYYYMMDD-YYYYMMDD_timestamp.csv
        import re
        
        filename = path.split('/')[-1]
        match = re.search(r'_(\d{8})-(\d{8})_', filename)
        
        if match:
            start_str = match.group(1)
            end_str = match.group(2)
            try:
                start_date = datetime.strptime(start_str, '%Y%m%d')
                end_date = datetime.strptime(end_str, '%Y%m%d')
                return (start_date, end_date)
            except ValueError:
                pass
        
        return None


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
    top_brands = df.nlargest(top_n, 'Net Sales').copy()
    
    # Convert margin to percentage for display
    top_brands['Margin_Pct'] = top_brands['Gross Margin %'] * 100
    
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # Bar chart for Net Sales
    fig.add_trace(
        go.Bar(
            x=top_brands['Brand'], 
            y=top_brands['Net Sales'],
            name='Net Sales', 
            marker_color='steelblue',
            hovertemplate='<b>%{x}</b><br>Net Sales: $%{y:,.0f}<extra></extra>'
        ),
        secondary_y=False
    )
    
    # Scatter plot for Gross Margin (markers only, no line)
    fig.add_trace(
        go.Scatter(
            x=top_brands['Brand'], 
            y=top_brands['Margin_Pct'],
            name='Gross Margin %', 
            mode='markers',  # Markers only - no connecting line
            marker=dict(
                color='coral',
                size=12,
                symbol='diamond',
                line=dict(width=1, color='white')
            ),
            hovertemplate='<b>%{x}</b><br>Margin: %{y:.1f}%<extra></extra>'
        ),
        secondary_y=True
    )
    
    # Add a reference line for target margin (e.g., 55%)
    fig.add_hline(
        y=55, 
        line_dash="dash", 
        line_color="rgba(255,255,255,0.3)",
        secondary_y=True,
        annotation_text="55% Target",
        annotation_position="right"
    )
    
    fig.update_layout(
        title=f'Top {top_n} Brands by Net Sales with Margin Overlay',
        xaxis_tickangle=-45,
        height=500,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        hovermode='x unified'
    )
    fig.update_yaxes(title_text="Net Sales ($)", secondary_y=False)
    fig.update_yaxes(title_text="Gross Margin (%)", range=[40, 90], secondary_y=True)
    
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
    # Use a flag to track if we've attempted to load from S3 this session
    if 'data_loaded_from_s3' not in st.session_state:
        st.session_state.data_loaded_from_s3 = False
    
    if 'sales_data' not in st.session_state:
        st.session_state.sales_data = None
    if 'brand_data' not in st.session_state:
        st.session_state.brand_data = None
    if 'product_data' not in st.session_state:
        st.session_state.product_data = None
    if 'brand_product_mapping' not in st.session_state:
        st.session_state.brand_product_mapping = None
    
    # Auto-load data from S3 on first run of session
    if not st.session_state.data_loaded_from_s3:
        with st.spinner("🔄 Loading data from S3..."):
            # Load brand-product mapping
            st.session_state.brand_product_mapping = s3_manager.load_brand_product_mapping()
            
            # Load all CSV data
            loaded_data = s3_manager.load_all_data_from_s3(processor)
            
            if loaded_data['sales'] is not None:
                st.session_state.sales_data = loaded_data['sales']
            if loaded_data['brand'] is not None:
                st.session_state.brand_data = loaded_data['brand']
            if loaded_data['product'] is not None:
                st.session_state.product_data = loaded_data['product']
            
            st.session_state.data_loaded_from_s3 = True
            
            # Show what was loaded
            loaded_items = []
            if st.session_state.sales_data is not None:
                loaded_items.append(f"Sales ({len(st.session_state.sales_data)} records)")
            if st.session_state.brand_data is not None:
                loaded_items.append(f"Brands ({len(st.session_state.brand_data)} records)")
            if st.session_state.product_data is not None:
                loaded_items.append(f"Products ({len(st.session_state.product_data)} records)")
            if st.session_state.brand_product_mapping:
                loaded_items.append(f"Mappings ({len(st.session_state.brand_product_mapping)} brands)")
            
            if loaded_items:
                st.toast(f"✅ Loaded: {', '.join(loaded_items)}", icon="📊")
    
    # Sidebar
    with st.sidebar:
        st.image("https://via.placeholder.com/150x50?text=Your+Logo", width=150)
        st.markdown(f"**Logged in as:** {st.session_state.get('logged_in_user', 'Unknown')}")
        st.markdown("---")
        
        # Navigation
        nav_options = [
            "📊 Dashboard",
            "📈 Sales Analysis",
            "🏷️ Brand Performance",
            "📦 Product Categories",
            "🔗 Brand-Product Mapping",
            "💡 Recommendations",
        ]

        # Add research page if available
        if RESEARCH_AVAILABLE:
            nav_options.append("🔬 Industry Research")

        # Add manual research if available
        if MANUAL_RESEARCH_AVAILABLE:
            nav_options.append("📄 Manual Research")

        # Add SEO page if available
        if SEO_AVAILABLE:
            nav_options.append("🔍 SEO Analysis")

        nav_options.append("📤 Data Upload")

        page = st.radio("Navigation", nav_options)
        
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
        render_brand_analysis(st.session_state, analytics, selected_store, date_range)
    
    elif page == "📦 Product Categories":
        render_product_analysis(st.session_state, selected_store, date_range)
    
    elif page == "🔗 Brand-Product Mapping":
        render_brand_product_mapping(st.session_state, s3_manager)
    
    elif page == "💡 Recommendations":
        render_recommendations(st.session_state, analytics)

    elif page == "🔬 Industry Research":
        if RESEARCH_AVAILABLE:
            render_research_page()
        else:
            st.error("Research integration module not found. Make sure `research_integration.py` is in the same directory.")

    elif page == "📄 Manual Research":
        if MANUAL_RESEARCH_AVAILABLE:
            render_manual_research_page()
        else:
            st.error("Manual research integration module not found. Make sure `manual_research_integration.py` is in the same directory.")

    elif page == "🔍 SEO Analysis":
        if SEO_AVAILABLE:
            render_seo_page()
        else:
            st.error("SEO integration module not found. Make sure `seo_integration.py` is in the same directory.")

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


def render_brand_analysis(state, analytics, store_filter, date_filter=None):
    """Render brand performance analysis page."""
    st.header("Brand Performance Analysis")
    
    if state.brand_data is None:
        st.warning("Please upload brand data first.")
        return
    
    df = state.brand_data.copy()
    
    # Show available date ranges in the data
    if 'Upload_Start_Date' in df.columns and 'Upload_End_Date' in df.columns:
        date_ranges = df.groupby(['Upload_Start_Date', 'Upload_End_Date', 'Upload_Store']).size().reset_index(name='records')
        
        with st.expander("📅 Available Data Periods", expanded=False):
            for _, row in date_ranges.iterrows():
                start = row['Upload_Start_Date'].strftime('%m/%d/%Y') if pd.notna(row['Upload_Start_Date']) else 'Unknown'
                end = row['Upload_End_Date'].strftime('%m/%d/%Y') if pd.notna(row['Upload_End_Date']) else 'Unknown'
                store = row['Upload_Store']
                st.text(f"  • {store}: {start} - {end} ({row['records']} brands)")
        
        # Filter by date range if provided
        if date_filter and len(date_filter) == 2:
            filter_start, filter_end = date_filter
            filter_start = pd.to_datetime(filter_start)
            filter_end = pd.to_datetime(filter_end)
            
            # Keep data where upload period overlaps with filter period
            df = df[
                (df['Upload_Start_Date'] <= filter_end) & 
                (df['Upload_End_Date'] >= filter_start)
            ]
            
            if len(df) == 0:
                st.warning(f"No brand data available for the selected date range ({filter_start.strftime('%m/%d/%Y')} - {filter_end.strftime('%m/%d/%Y')})")
                return
            
            st.info(f"📅 Showing data for: {filter_start.strftime('%m/%d/%Y')} - {filter_end.strftime('%m/%d/%Y')}")
    
    # Filter by store if specified
    if store_filter and store_filter != 'All Stores':
        store_id = [k for k, v in STORE_DISPLAY_NAMES.items() if v == store_filter]
        if store_id and 'Upload_Store' in df.columns:
            df = df[df['Upload_Store'] == store_id[0]]
    
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
    significant_brands['Margin_Pct'] = significant_brands['Gross Margin %'] * 100
    
    # Color by margin performance
    fig = px.scatter(
        significant_brands, 
        x='Net Sales', 
        y='Margin_Pct',
        hover_name='Brand',
        color='Margin_Pct',
        color_continuous_scale='RdYlGn',  # Red (low) to Green (high)
        size='Net Sales',
        size_max=30,
        title='Brand Positioning: Sales vs Margin',
        log_x=True,
        labels={'Margin_Pct': 'Gross Margin %', 'Net Sales': 'Net Sales ($)'}
    )
    
    # Add quadrant lines
    fig.add_hline(
        y=55, 
        line_dash="dash", 
        line_color="rgba(255,255,255,0.5)", 
        annotation_text="55% Target Margin",
        annotation_position="right"
    )
    
    fig.update_layout(
        height=500,
        coloraxis_colorbar=dict(title="Margin %")
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Add interpretation help
    with st.expander("📖 How to read this chart"):
        st.markdown("""
        - **X-axis (horizontal)**: Net Sales in dollars (log scale)
        - **Y-axis (vertical)**: Gross Margin percentage
        - **Bubble size**: Larger = higher sales volume
        - **Color**: Green = high margin, Red = low margin
        
        **Quadrants:**
        - **Top-right**: Stars (high sales + high margin) ✅
        - **Top-left**: Niche winners (low sales but high margin)
        - **Bottom-right**: Volume drivers (high sales but low margin) - watch closely
        - **Bottom-left**: Consider discontinuing ⚠️
        """)


def render_product_analysis(state, store_filter=None, date_filter=None):
    """Render product category analysis page."""
    st.header("Product Category Analysis")
    
    if state.product_data is None:
        st.warning("Please upload product data first.")
        return
    
    df = state.product_data.copy()
    
    # Show available date ranges in the data
    if 'Upload_Start_Date' in df.columns and 'Upload_End_Date' in df.columns:
        date_ranges = df.groupby(['Upload_Start_Date', 'Upload_End_Date', 'Upload_Store']).size().reset_index(name='records')
        
        with st.expander("📅 Available Data Periods", expanded=False):
            for _, row in date_ranges.iterrows():
                start = row['Upload_Start_Date'].strftime('%m/%d/%Y') if pd.notna(row['Upload_Start_Date']) else 'Unknown'
                end = row['Upload_End_Date'].strftime('%m/%d/%Y') if pd.notna(row['Upload_End_Date']) else 'Unknown'
                store = row['Upload_Store']
                st.text(f"  • {store}: {start} - {end} ({row['records']} categories)")
        
        # Filter by date range if provided
        if date_filter and len(date_filter) == 2:
            filter_start, filter_end = date_filter
            filter_start = pd.to_datetime(filter_start)
            filter_end = pd.to_datetime(filter_end)
            
            # Keep data where upload period overlaps with filter period
            df = df[
                (df['Upload_Start_Date'] <= filter_end) & 
                (df['Upload_End_Date'] >= filter_start)
            ]
            
            if len(df) == 0:
                st.warning(f"No product data available for the selected date range ({filter_start.strftime('%m/%d/%Y')} - {filter_end.strftime('%m/%d/%Y')})")
                return
            
            st.info(f"📅 Showing data for: {filter_start.strftime('%m/%d/%Y')} - {filter_end.strftime('%m/%d/%Y')}")
    
    # Filter by store if specified
    if store_filter and store_filter != 'All Stores':
        store_id = [k for k, v in STORE_DISPLAY_NAMES.items() if v == store_filter]
        if store_id and 'Upload_Store' in df.columns:
            df = df[df['Upload_Store'] == store_id[0]]
    
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
    
    # Generate rule-based recommendations
    metrics = analytics.calculate_store_metrics(state.sales_data)
    recommendations = analytics.generate_recommendations(metrics, state.brand_data)
    
    # Create tabs for different recommendation types
    tab1, tab2 = st.tabs(["📋 Automated Insights", "🤖 AI Analysis"])
    
    with tab1:
        if not recommendations:
            st.info("No specific recommendations at this time. Data looks good!")
        else:
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
    
    with tab2:
        # Claude AI Integration
        if not CLAUDE_AVAILABLE:
            st.warning("Claude integration module not found. Make sure `claude_integration.py` is in the same directory.")
            return

        # Get API key from environment variable or secrets
        api_key = os.environ.get("ANTHROPIC_API_KEY")

        # Fallback to secrets if environment variable not set
        if not api_key:
            try:
                api_key = st.secrets.get("ANTHROPIC_API_KEY")
            except Exception:
                pass

        if not api_key:
            st.info("""
            **🔑 Enable AI Analysis**

            Add your Anthropic API key to unlock AI-powered insights:

            **Option 1: Environment Variable (Recommended)**
            ```bash
            export ANTHROPIC_API_KEY="sk-ant-api03-..."
            ```

            **Option 2: Streamlit Secrets**
            Add to `.streamlit/secrets.toml`:
            ```toml
            ANTHROPIC_API_KEY = "sk-ant-api03-..."
            ```
            """)
            return

        # Initialize Claude
        claude = ClaudeAnalytics(api_key=api_key)
        
        if not claude.is_available():
            st.error("Could not initialize Claude API. Please check your API key.")
            return
        
        st.success("✅ Claude AI connected")
        
        # Get brand-product mapping
        brand_product_mapping = state.brand_product_mapping or {}
        mapping_count = len(brand_product_mapping)
        
        if mapping_count > 0:
            st.info(f"🔗 Using {mapping_count} brand-product mappings for enhanced analysis")
        else:
            st.caption("💡 Tip: Set up Brand-Product Mappings for more detailed category insights")
        
        # Prepare data summaries for Claude
        sales_summary = {
            'store_metrics': metrics,
            'date_range': {
                'start': state.sales_data['Date'].min().strftime('%Y-%m-%d') if 'Date' in state.sales_data.columns else 'Unknown',
                'end': state.sales_data['Date'].max().strftime('%Y-%m-%d') if 'Date' in state.sales_data.columns else 'Unknown',
            },
            'total_records': len(state.sales_data)
        }
        
        # Enrich brand data with product category mapping
        brand_summary = []
        brand_by_category = {}
        if state.brand_data is not None:
            # Filter out promotional brands (containing "$1" or "Promo")
            filtered_brands = state.brand_data[
                ~state.brand_data['Brand'].str.contains(r'\$1|Promo', case=False, na=False, regex=True)
            ]

            top_brands_df = filtered_brands.nlargest(30, 'Net Sales')[['Brand', 'Net Sales', 'Gross Margin %']].copy()

            # Add product category from mapping
            top_brands_df['Product_Category'] = top_brands_df['Brand'].map(brand_product_mapping).fillna('Unmapped')
            brand_summary = top_brands_df.to_dict('records')
            
            # Aggregate by category for category-level insights
            if brand_product_mapping:
                all_brands_df = filtered_brands.copy()
                all_brands_df['Product_Category'] = all_brands_df['Brand'].map(brand_product_mapping).fillna('Unmapped')
                
                category_agg = all_brands_df.groupby('Product_Category').agg({
                    'Net Sales': 'sum',
                    'Gross Margin %': 'mean',
                    'Brand': 'count'
                }).rename(columns={'Brand': 'Brand_Count'}).reset_index()
                
                brand_by_category = category_agg.to_dict('records')
        
        # AI Analysis Buttons
        col1, col2, col3, col4 = st.columns(4)
        
        # Initialize session state for AI analysis result
        if 'ai_analysis_result' not in st.session_state:
            st.session_state.ai_analysis_result = None
            st.session_state.ai_analysis_title = None
        
        with col1:
            if st.button("📊 Analyze Sales Trends", use_container_width=True):
                with st.spinner("Claude is analyzing your sales data..."):
                    analysis = claude.analyze_sales_trends(sales_summary)
                    st.session_state.ai_analysis_title = "📊 Sales Analysis"
                    st.session_state.ai_analysis_result = analysis
                    st.rerun()
        
        with col2:
            if st.button("🏷️ Brand Recommendations", use_container_width=True):
                if not brand_summary:
                    st.warning("Upload brand data first.")
                else:
                    with st.spinner("Claude is analyzing brand performance..."):
                        analysis = claude.analyze_brand_performance(brand_summary, brand_by_category)
                        st.session_state.ai_analysis_title = "🏷️ Brand Analysis"
                        st.session_state.ai_analysis_result = analysis
                        st.rerun()
        
        with col3:
            if st.button("📦 Category Insights", use_container_width=True):
                if not brand_by_category:
                    st.warning("Set up brand-product mappings first to get category insights.")
                else:
                    with st.spinner("Claude is analyzing category performance..."):
                        analysis = claude.analyze_category_performance(brand_by_category, brand_summary)
                        st.session_state.ai_analysis_title = "📦 Category Analysis"
                        st.session_state.ai_analysis_result = analysis
                        st.rerun()
        
        with col4:
            if st.button("🎯 Deal Suggestions", use_container_width=True):
                if state.brand_data is None:
                    st.warning("Upload brand data first.")
                else:
                    with st.spinner("Claude is generating deal recommendations..."):
                        # Find slow movers and high margin items with category info
                        slow_df = state.brand_data.nsmallest(15, 'Net Sales')[['Brand', 'Net Sales', 'Gross Margin %']].copy()
                        slow_df['Product_Category'] = slow_df['Brand'].map(brand_product_mapping).fillna('Unmapped')
                        slow_movers = slow_df.to_dict('records')
                        
                        high_df = state.brand_data[state.brand_data['Gross Margin %'] > 0.6].nlargest(15, 'Net Sales')[['Brand', 'Net Sales', 'Gross Margin %']].copy()
                        high_df['Product_Category'] = high_df['Brand'].map(brand_product_mapping).fillna('Unmapped')
                        high_margin = high_df.to_dict('records')
                        
                        analysis = claude.generate_deal_recommendations(slow_movers, high_margin)
                        st.session_state.ai_analysis_title = "🎯 Deal Recommendations"
                        st.session_state.ai_analysis_result = analysis
                        st.rerun()
        
        # Display AI analysis result at full width
        if st.session_state.ai_analysis_result:
            st.markdown("---")
            
            # Header with clear button
            header_col1, header_col2 = st.columns([6, 1])
            with header_col1:
                st.subheader(st.session_state.ai_analysis_title)
            with header_col2:
                if st.button("✖️ Clear", key="clear_analysis"):
                    st.session_state.ai_analysis_result = None
                    st.session_state.ai_analysis_title = None
                    st.rerun()
            
            # Full-width analysis content
            st.markdown(st.session_state.ai_analysis_result)
        
        # Free-form Q&A
        st.markdown("---")
        st.subheader("💬 Ask Claude About Your Business")
        
        question = st.text_input("Ask anything about your sales, brands, or business strategy:")
        
        if question:
            # Prepare context with mapping
            context = {
                'sales_summary': sales_summary,
                'top_brands': brand_summary[:20] if brand_summary else [],
                'product_mix': state.product_data.to_dict('records') if state.product_data is not None else [],
                'brand_by_category': brand_by_category if brand_by_category else [],
                'brand_product_mapping_sample': dict(list(brand_product_mapping.items())[:30]) if brand_product_mapping else {}
            }
            
            with st.spinner("Thinking..."):
                answer = claude.answer_business_question(question, context)
                st.markdown("### Answer")
                st.markdown(answer)


def render_brand_product_mapping(state, s3_manager):
    """Render brand-product mapping configuration page."""
    st.header("🔗 Brand-Product Mapping")
    
    st.markdown("""
    Link brands to their product categories. This helps the AI provide more accurate 
    insights by understanding which brands sell which types of products.
    """)
    
    # Check if we have brand data
    if state.brand_data is None:
        st.warning("Please upload brand data first to set up mappings.")
        return
    
    # Get unique brands from the data
    brands = state.brand_data['Brand'].unique().tolist()
    brands = sorted([b for b in brands if pd.notna(b) and str(b).strip()])
    
    # Define product categories (from your data)
    product_categories = [
        "FLOWER",
        "PREROLL", 
        "CARTRIDGE",
        "EDIBLE",
        "EXTRACT",
        "BEVERAGE",
        "TINCTURE",
        "TOPICAL",
        "PILL",
        "MERCH",
        "OTHER"
    ]
    
    # Get current mapping from session state
    current_mapping = state.brand_product_mapping or {}
    
    # Stats
    mapped_count = len([b for b in brands if b in current_mapping])
    st.info(f"📊 **{mapped_count}** of **{len(brands)}** brands mapped ({100*mapped_count/len(brands):.1f}%)")
    
    # Tabs for different views
    tab1, tab2, tab3 = st.tabs(["🔧 Quick Mapping", "📋 Bulk Edit", "📊 View Mappings"])
    
    with tab1:
        st.subheader("Quick Mapping")
        st.markdown("Select a brand and assign it to a product category.")
        
        col1, col2, col3 = st.columns([2, 2, 1])
        
        with col1:
            # Filter for unmapped brands option
            show_unmapped_only = st.checkbox("Show unmapped brands only", value=True)
            
            if show_unmapped_only:
                available_brands = [b for b in brands if b not in current_mapping]
            else:
                available_brands = brands
            
            if not available_brands:
                st.success("✅ All brands are mapped!")
                selected_brand = None
            else:
                selected_brand = st.selectbox(
                    "Select Brand",
                    options=available_brands,
                    key="quick_map_brand"
                )
        
        with col2:
            if selected_brand:
                # Show current mapping if exists
                current_cat = current_mapping.get(selected_brand, None)
                default_idx = product_categories.index(current_cat) if current_cat in product_categories else 0
                
                selected_category = st.selectbox(
                    "Assign to Category",
                    options=product_categories,
                    index=default_idx,
                    key="quick_map_category"
                )
        
        with col3:
            if selected_brand:
                st.markdown("<br>", unsafe_allow_html=True)
                if st.button("💾 Save", key="quick_save", use_container_width=True):
                    current_mapping[selected_brand] = selected_category
                    state.brand_product_mapping = current_mapping
                    
                    # Save to S3
                    success, message = s3_manager.save_brand_product_mapping(current_mapping)
                    if success:
                        st.success(f"✅ Mapped '{selected_brand}' → {selected_category}")
                    else:
                        st.warning(f"Saved locally. S3: {message}")
                    st.rerun()
    
    with tab2:
        st.subheader("Bulk Edit")
        st.markdown("Edit multiple brand mappings at once. Changes are saved when you click 'Save All'.")
        
        # Filter options
        filter_col1, filter_col2, filter_col3 = st.columns(3)
        with filter_col1:
            filter_category = st.selectbox(
                "Filter by category",
                options=["All Categories"] + product_categories + ["Unmapped"],
                key="bulk_filter_cat"
            )
        with filter_col2:
            search_term = st.text_input("Search brands", key="bulk_search")
        with filter_col3:
            sort_option = st.selectbox(
                "Sort by",
                options=["Alphabetical", "Mapped First", "Unmapped First"],
                key="bulk_sort"
            )

        # Filter brands
        filtered_brands = brands.copy()

        if filter_category == "Unmapped":
            filtered_brands = [b for b in filtered_brands if b not in current_mapping]
        elif filter_category != "All Categories":
            filtered_brands = [b for b in filtered_brands if current_mapping.get(b) == filter_category]

        if search_term:
            filtered_brands = [b for b in filtered_brands if search_term.lower() in b.lower()]

        # Sort brands based on selected option
        if sort_option == "Mapped First":
            filtered_brands = sorted(filtered_brands, key=lambda b: (b not in current_mapping, b.lower()))
        elif sort_option == "Unmapped First":
            filtered_brands = sorted(filtered_brands, key=lambda b: (b in current_mapping, b.lower()))
        else:  # Alphabetical
            filtered_brands = sorted(filtered_brands, key=lambda b: b.lower())
        
        st.caption(f"Showing {len(filtered_brands)} brands")
        
        # Create editable mapping
        if filtered_brands:
            # Show in batches of 20
            batch_size = 20
            total_batches = (len(filtered_brands) + batch_size - 1) // batch_size
            
            if total_batches > 1:
                batch_num = st.number_input("Page", min_value=1, max_value=total_batches, value=1, key="bulk_page")
            else:
                batch_num = 1
            
            start_idx = (batch_num - 1) * batch_size
            end_idx = min(start_idx + batch_size, len(filtered_brands))
            batch_brands = filtered_brands[start_idx:end_idx]
            
            # Store changes temporarily
            if 'bulk_changes' not in st.session_state:
                st.session_state.bulk_changes = {}
            
            for brand in batch_brands:
                col1, col2 = st.columns([3, 2])
                with col1:
                    st.text(brand)
                with col2:
                    current_cat = current_mapping.get(brand, "")
                    default_idx = product_categories.index(current_cat) if current_cat in product_categories else len(product_categories)
                    
                    new_cat = st.selectbox(
                        "Category",
                        options=product_categories + ["-- Not Mapped --"],
                        index=default_idx if current_cat else len(product_categories),
                        key=f"bulk_{brand}",
                        label_visibility="collapsed"
                    )
                    
                    if new_cat != "-- Not Mapped --":
                        st.session_state.bulk_changes[brand] = new_cat
                    elif brand in st.session_state.bulk_changes:
                        del st.session_state.bulk_changes[brand]
            
            st.markdown("---")
            
            if st.button("💾 Save All Changes", key="bulk_save", type="primary"):
                # Apply all changes
                for brand, category in st.session_state.bulk_changes.items():
                    current_mapping[brand] = category
                
                state.brand_product_mapping = current_mapping
                
                # Save to S3
                success, message = s3_manager.save_brand_product_mapping(current_mapping)
                if success:
                    st.success(f"✅ Saved {len(st.session_state.bulk_changes)} mappings")
                else:
                    st.warning(f"Saved locally. S3: {message}")
                
                st.session_state.bulk_changes = {}
                st.rerun()
    
    with tab3:
        st.subheader("Current Mappings")
        
        if not current_mapping:
            st.info("No mappings configured yet. Use the Quick Mapping or Bulk Edit tabs to get started.")
        else:
            # Group by category
            by_category = {}
            for brand, category in current_mapping.items():
                if category not in by_category:
                    by_category[category] = []
                by_category[category].append(brand)
            
            # Display summary
            summary_data = []
            for cat in product_categories:
                brands_in_cat = by_category.get(cat, [])
                summary_data.append({
                    "Category": cat,
                    "Brand Count": len(brands_in_cat),
                    "Brands": ", ".join(sorted(brands_in_cat)[:5]) + ("..." if len(brands_in_cat) > 5 else "")
                })
            
            st.dataframe(
                pd.DataFrame(summary_data),
                use_container_width=True,
                hide_index=True
            )
            
            # Export/Import options
            st.markdown("---")
            col1, col2 = st.columns(2)
            
            with col1:
                # Export
                mapping_json = json.dumps(current_mapping, indent=2)
                st.download_button(
                    "📥 Export Mappings (JSON)",
                    mapping_json,
                    "brand_product_mapping.json",
                    "application/json"
                )
            
            with col2:
                # Import
                uploaded_mapping = st.file_uploader(
                    "📤 Import Mappings (JSON)",
                    type=['json'],
                    key="import_mapping"
                )
                
                if uploaded_mapping:
                    try:
                        imported = json.load(uploaded_mapping)
                        if isinstance(imported, dict):
                            if st.button("Apply Imported Mappings"):
                                state.brand_product_mapping = imported
                                success, message = s3_manager.save_brand_product_mapping(imported)
                                if success:
                                    st.success(f"✅ Imported {len(imported)} mappings")
                                else:
                                    st.warning(f"Saved locally. S3: {message}")
                                st.rerun()
                        else:
                            st.error("Invalid mapping format")
                    except json.JSONDecodeError:
                        st.error("Invalid JSON file")
            
            # Clear all option
            st.markdown("---")
            if st.button("🗑️ Clear All Mappings", type="secondary"):
                state.brand_product_mapping = {}
                s3_manager.save_brand_product_mapping({})
                st.success("All mappings cleared")
                st.rerun()


def render_upload_page(s3_manager, processor):
    """Render data upload page."""
    st.header("📤 Data Upload")
    
    # S3 Connection Status
    st.subheader("☁️ S3 Storage Status")
    
    s3_connected, s3_message = s3_manager.test_connection()
    
    if s3_connected:
        st.success(f"✅ {s3_message}")
        
        # Show existing files in bucket
        with st.expander("📂 Files in S3 Bucket"):
            files = s3_manager.list_files()
            if files:
                for f in files[:50]:  # Limit to 50 files
                    st.text(f"  {f}")
                if len(files) > 50:
                    st.text(f"  ... and {len(files) - 50} more files")
            else:
                st.info("Bucket is empty - no files uploaded yet")
    else:
        st.error(f"❌ S3 Not Connected: {s3_message}")
        st.markdown("""
        **To fix this, check your `.streamlit/secrets.toml`:**
        ```toml
        [aws]
        access_key_id = "AKIA..."
        secret_access_key = "your_secret_key..."
        region = "us-west-2"
        bucket_name = "retail-data-bcgr"
        ```
        
        Make sure:
        1. All four values are filled in
        2. The IAM user has `s3:PutObject`, `s3:GetObject`, `s3:ListBucket` permissions
        3. The bucket name matches exactly (case-sensitive)
        """)
    
    st.markdown("---")
    
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
        # Default date range (last 30 days)
        default_start = (datetime.now() - timedelta(days=30)).strftime("%m/%d/%Y")
        default_end = datetime.now().strftime("%m/%d/%Y")
        default_range = f"{default_start} - {default_end}"
        
        date_range_input = st.text_input(
            "Data Date Range",
            value=default_range,
            help="Format: MM/DD/YYYY - MM/DD/YYYY",
            placeholder="MM/DD/YYYY - MM/DD/YYYY"
        )
        
        # Parse the date range
        start_date = None
        end_date = None
        date_error = False
        
        try:
            if " - " in date_range_input:
                start_str, end_str = date_range_input.split(" - ")
                start_date = datetime.strptime(start_str.strip(), "%m/%d/%Y").date()
                end_date = datetime.strptime(end_str.strip(), "%m/%d/%Y").date()
            else:
                # Single date entered
                start_date = datetime.strptime(date_range_input.strip(), "%m/%d/%Y").date()
                end_date = start_date
        except ValueError:
            date_error = True
            st.error("⚠️ Invalid date format. Please use MM/DD/YYYY - MM/DD/YYYY")
            # Fallback to defaults
            start_date = (datetime.now() - timedelta(days=30)).date()
            end_date = datetime.now().date()
    
    # Display selected settings
    if not date_error:
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
                
                success, message = s3_manager.upload_file(sales_file, s3_key)
                if success:
                    st.success(f"✅ {message}")
                else:
                    st.warning(f"⚠️ S3 upload failed: {message}")
                    st.info("Data processed locally but NOT saved to S3")
                
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
                
                success, message = s3_manager.upload_file(brand_file, s3_key)
                if success:
                    st.success(f"✅ {message}")
                else:
                    st.warning(f"⚠️ S3 upload failed: {message}")
                    st.info("Data processed locally but NOT saved to S3")
                
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
                
                success, message = s3_manager.upload_file(product_file, s3_key)
                if success:
                    st.success(f"✅ {message}")
                else:
                    st.warning(f"⚠️ S3 upload failed: {message}")
                    st.info("Data processed locally but NOT saved to S3")
                
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
                st.caption(f"📅 {df['Date'].min().strftime('%m/%d/%Y')} - {df['Date'].max().strftime('%m/%d/%Y')}")
        else:
            st.warning("❌ No sales data loaded")
    
    with status_col2:
        if st.session_state.brand_data is not None:
            df = st.session_state.brand_data
            stores = df['Upload_Store'].unique().tolist() if 'Upload_Store' in df.columns else ['Unknown']
            st.success(f"✅ Brands: {len(df)} records")
            st.caption(f"Stores: {', '.join(stores)}")
            if 'Upload_Start_Date' in df.columns and 'Upload_End_Date' in df.columns:
                # Show all unique date ranges
                for store in stores:
                    store_df = df[df['Upload_Store'] == store] if 'Upload_Store' in df.columns else df
                    start = store_df['Upload_Start_Date'].min()
                    end = store_df['Upload_End_Date'].max()
                    if pd.notna(start) and pd.notna(end):
                        st.caption(f"📅 {store}: {start.strftime('%m/%d/%Y')} - {end.strftime('%m/%d/%Y')}")
        else:
            st.warning("❌ No brand data loaded")
    
    with status_col3:
        if st.session_state.product_data is not None:
            df = st.session_state.product_data
            stores = df['Upload_Store'].unique().tolist() if 'Upload_Store' in df.columns else ['Unknown']
            st.success(f"✅ Products: {len(df)} records")
            st.caption(f"Stores: {', '.join(stores)}")
            if 'Upload_Start_Date' in df.columns and 'Upload_End_Date' in df.columns:
                # Show all unique date ranges
                for store in stores:
                    store_df = df[df['Upload_Store'] == store] if 'Upload_Store' in df.columns else df
                    start = store_df['Upload_Start_Date'].min()
                    end = store_df['Upload_End_Date'].max()
                    if pd.notna(start) and pd.notna(end):
                        st.caption(f"📅 {store}: {start.strftime('%m/%d/%Y')} - {end.strftime('%m/%d/%Y')}")
        else:
            st.warning("❌ No product data loaded")
    
    # Data management section
    st.markdown("---")
    st.subheader("🗂️ Data Management")
    
    mgmt_col1, mgmt_col2, mgmt_col3 = st.columns(3)
    
    with mgmt_col1:
        if st.button("🔄 Reload from S3", type="primary", use_container_width=True):
            with st.spinner("Reloading data from S3..."):
                # Reload all data from S3
                loaded_data = s3_manager.load_all_data_from_s3(processor)
                
                if loaded_data['sales'] is not None:
                    st.session_state.sales_data = loaded_data['sales']
                if loaded_data['brand'] is not None:
                    st.session_state.brand_data = loaded_data['brand']
                if loaded_data['product'] is not None:
                    st.session_state.product_data = loaded_data['product']
                
                # Also reload mappings
                st.session_state.brand_product_mapping = s3_manager.load_brand_product_mapping()
                
                # Show what was loaded
                loaded_items = []
                if st.session_state.sales_data is not None:
                    loaded_items.append(f"Sales ({len(st.session_state.sales_data)})")
                if st.session_state.brand_data is not None:
                    loaded_items.append(f"Brands ({len(st.session_state.brand_data)})")
                if st.session_state.product_data is not None:
                    loaded_items.append(f"Products ({len(st.session_state.product_data)})")
                
                if loaded_items:
                    st.success(f"✅ Reloaded: {', '.join(loaded_items)}")
                else:
                    st.info("No data found in S3")
                st.rerun()
    
    with mgmt_col2:
        if st.button("🗑️ Clear Session Data", type="secondary", use_container_width=True):
            st.session_state.sales_data = None
            st.session_state.brand_data = None
            st.session_state.product_data = None
            st.session_state.data_loaded_from_s3 = False
            st.success("Session data cleared! (S3 data preserved)")
            st.rerun()
    
    with mgmt_col3:
        if st.session_state.sales_data is not None or st.session_state.brand_data is not None:
            if st.button("📥 Export Summary", use_container_width=True):
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
