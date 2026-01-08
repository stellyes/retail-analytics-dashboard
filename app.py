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

# Import all services from the dashboard package
from dashboard import (
    # Service availability flags
    CLAUDE_AVAILABLE,
    INVOICE_AVAILABLE as INVOICE_DATA_AVAILABLE,
    RESEARCH_AVAILABLE,
    SEO_AVAILABLE,
    MANUAL_RESEARCH_AVAILABLE,
    QR_AVAILABLE,
    BUSINESS_CONTEXT_AVAILABLE,
    INVOICE_UPLOAD_AVAILABLE,
    # Claude AI
    ClaudeAnalytics,
    # Invoice
    InvoiceDataService,
    # Research
    render_research_page,
    ResearchFindingsViewer,
    # SEO
    render_seo_page,
    SEOFindingsViewer,
    # Manual Research
    MonthlyResearchSummarizer,
    DocumentStorage,
    S3_BUCKET,
    # QR
    render_qr_page,
    # Business Context
    BusinessContextService,
    get_business_context_service,
    # Invoice Upload UI
    render_full_invoice_section,
)

# =============================================================================
# CONFIGURATION
# =============================================================================

st.set_page_config(
    page_title="Retail Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =============================================================================
# SMART CACHING - Hash-based cache invalidation with localStorage persistence
# =============================================================================

# Simple XOR encryption key for localStorage (adequate for internal apps)
_CACHE_KEY = "r3t41l_4n4lyt1cs_2024"

def _compute_data_hash(data: dict) -> str:
    """Compute a hash of the data for cache invalidation."""
    # Create a deterministic string representation
    json_str = json.dumps(data, sort_keys=True, default=str)
    return hashlib.md5(json_str.encode()).hexdigest()[:16]

def _xor_encrypt(data: str, key: str) -> str:
    """Simple XOR encryption for localStorage data."""
    import base64
    encrypted = ''.join(chr(ord(c) ^ ord(key[i % len(key)])) for i, c in enumerate(data))
    return base64.b64encode(encrypted.encode('latin-1')).decode('ascii')

def _xor_decrypt(data: str, key: str) -> str:
    """Simple XOR decryption for localStorage data."""
    import base64
    decoded = base64.b64decode(data.encode('ascii')).decode('latin-1')
    return ''.join(chr(ord(c) ^ ord(key[i % len(key)])) for i, c in enumerate(decoded))

def _save_hash_to_localstorage(hash_value: str):
    """Save data hash to localStorage for cache invalidation checks."""
    try:
        import streamlit.components.v1 as components
        components.html(f"""
            <script>
                try {{
                    localStorage.setItem('retail_data_hash', '{hash_value}');
                    localStorage.setItem('retail_cache_timestamp', '{datetime.now().isoformat()}');
                }} catch(e) {{
                    console.log('localStorage hash save failed:', e);
                }}
            </script>
        """, height=0)
    except Exception:
        pass

def _save_to_localstorage(key: str, data: dict):
    """Save data to browser localStorage with encryption."""
    try:
        import streamlit.components.v1 as components
        json_data = json.dumps(data, default=str)
        encrypted = _xor_encrypt(json_data, _CACHE_KEY)
        # Compute and save hash along with data
        data_hash = _compute_data_hash(data)
        components.html(f"""
            <script>
                try {{
                    localStorage.setItem('retail_cache_{key}', '{encrypted}');
                    localStorage.setItem('retail_cache_{key}_hash', '{data_hash}');
                    localStorage.setItem('retail_cache_{key}_timestamp', '{datetime.now().isoformat()}');
                }} catch(e) {{
                    console.log('localStorage save failed:', e);
                }}
            </script>
        """, height=0)
    except Exception:
        pass

def _get_localstorage_hash_check_js():
    """Generate JavaScript that reads localStorage hash and sends it to Streamlit via query params."""
    return """
    <script>
        (function() {
            try {
                const hash = localStorage.getItem('retail_data_hash') || '';
                const timestamp = localStorage.getItem('retail_cache_timestamp') || '';
                // Store in sessionStorage so Streamlit can potentially read it
                sessionStorage.setItem('cache_hash', hash);
                sessionStorage.setItem('cache_timestamp', timestamp);
                // Also try to communicate via a custom event
                window.postMessage({type: 'RETAIL_CACHE_HASH', hash: hash, timestamp: timestamp}, '*');
            } catch(e) {
                console.log('localStorage hash check failed:', e);
            }
        })();
    </script>
    """

def _show_loading_overlay(message: str = "Syncing data...", submessage: str = "New data detected in cloud"):
    """Show a fullscreen loading overlay with progress animation.

    Uses st.markdown with a script to inject the overlay into the parent document body,
    bypassing Streamlit's iframe sandbox to cover the entire viewport.
    """
    # Generate unique ID to prevent duplicate overlays
    overlay_id = "retail-loading-overlay"

    # CSS and HTML for the overlay - injected via script into parent document
    overlay_html = f"""
    <script>
    (function() {{
        // Remove any existing overlay first
        const existing = document.getElementById('{overlay_id}');
        if (existing) existing.remove();

        // Create style element for animations
        const style = document.createElement('style');
        style.id = '{overlay_id}-styles';
        style.textContent = `
            @keyframes retail-pulse {{
                0%, 100% {{ opacity: 1; }}
                50% {{ opacity: 0.5; }}
            }}
            @keyframes retail-progress {{
                0% {{ width: 0%; }}
                50% {{ width: 70%; }}
                100% {{ width: 100%; }}
            }}
            @keyframes retail-spin {{
                0% {{ transform: rotate(0deg); }}
                100% {{ transform: rotate(360deg); }}
            }}
        `;
        document.head.appendChild(style);

        // Create overlay element
        const overlay = document.createElement('div');
        overlay.id = '{overlay_id}';
        overlay.innerHTML = `
            <div style="
                width: 60px;
                height: 60px;
                border: 4px solid rgba(255, 255, 255, 0.1);
                border-top: 4px solid #4CAF50;
                border-radius: 50%;
                animation: retail-spin 1s linear infinite;
                margin-bottom: 24px;
            "></div>
            <div style="
                color: white;
                font-size: 24px;
                font-weight: 600;
                margin-bottom: 8px;
                animation: retail-pulse 2s ease-in-out infinite;
            ">{message}</div>
            <div style="
                color: rgba(255, 255, 255, 0.7);
                font-size: 14px;
                margin-bottom: 32px;
            ">{submessage}</div>
            <div style="
                width: 300px;
                height: 6px;
                background: rgba(255, 255, 255, 0.1);
                border-radius: 3px;
                overflow: hidden;
            ">
                <div style="
                    height: 100%;
                    background: linear-gradient(90deg, #4CAF50, #8BC34A);
                    border-radius: 3px;
                    animation: retail-progress 3s ease-in-out infinite;
                "></div>
            </div>
            <div style="
                color: rgba(255, 255, 255, 0.5);
                font-size: 12px;
                margin-top: 16px;
            ">Please wait while we fetch the latest data...</div>
        `;

        // Apply styles to overlay container
        Object.assign(overlay.style, {{
            position: 'fixed',
            top: '0',
            left: '0',
            width: '100vw',
            height: '100vh',
            background: 'rgba(0, 0, 0, 0.85)',
            backdropFilter: 'blur(8px)',
            webkitBackdropFilter: 'blur(8px)',
            display: 'flex',
            flexDirection: 'column',
            justifyContent: 'center',
            alignItems: 'center',
            zIndex: '999999',
            fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif'
        }});

        // Append to body
        document.body.appendChild(overlay);

        // Auto-remove after 15 seconds as fallback
        setTimeout(function() {{
            const el = document.getElementById('{overlay_id}');
            if (el) {{
                el.style.transition = 'opacity 0.5s';
                el.style.opacity = '0';
                setTimeout(() => el.remove(), 500);
            }}
            const styleEl = document.getElementById('{overlay_id}-styles');
            if (styleEl) styleEl.remove();
        }}, 15000);
    }})();
    </script>
    """

    st.markdown(overlay_html, unsafe_allow_html=True)

def _hide_loading_overlay():
    """Hide the loading overlay."""
    overlay_id = "retail-loading-overlay"
    hide_script = f"""
    <script>
    (function() {{
        const overlay = document.getElementById('{overlay_id}');
        if (overlay) {{
            overlay.style.transition = 'opacity 0.3s';
            overlay.style.opacity = '0';
            setTimeout(() => overlay.remove(), 300);
        }}
        const style = document.getElementById('{overlay_id}-styles');
        if (style) style.remove();
    }})();
    </script>
    """
    st.markdown(hide_script, unsafe_allow_html=True)

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
        # Default passwords if secrets.toml not configured
        default_users = {
            "admin": hashlib.sha256("changeme123".encode()).hexdigest(),
            "analyst": hashlib.sha256("viewonly456".encode()).hexdigest()
        }

        try:
            users = st.secrets["passwords"]
        except:
            users = default_users

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
                    aws_secrets = st.secrets["aws"]
                    aws_access_key = aws_secrets.get("access_key_id")
                    aws_secret_key = aws_secrets.get("secret_access_key")
                    aws_region = aws_secrets.get("region", "us-west-2")
                    bucket_name = aws_secrets.get("bucket_name")
                except Exception as e:
                    # Secrets not configured, will use IAM role or fail gracefully
                    pass
            
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
            return pd.read_csv(io.BytesIO(response['Body'].read()), low_memory=False)
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

    def get_data_hash(self) -> str:
        """
        Get a hash representing the current state of all data files in S3.
        Uses S3 ETags (MD5 hashes) combined with file list to detect changes.
        This is a lightweight operation that doesn't download file contents.
        """
        if not self.is_configured():
            return ""

        try:
            # Get all files in raw-uploads with their ETags
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix="raw-uploads/"
            )

            if 'Contents' not in response:
                return "empty"

            # Build hash from file keys, ETags, and last modified times
            hash_parts = []
            for obj in sorted(response['Contents'], key=lambda x: x['Key']):
                # ETag is already an MD5 hash of the file content
                hash_parts.append(f"{obj['Key']}:{obj['ETag']}:{obj['LastModified'].isoformat()}")

            # Handle pagination for large buckets
            while response.get('IsTruncated', False):
                response = self.s3_client.list_objects_v2(
                    Bucket=self.bucket_name,
                    Prefix="raw-uploads/",
                    ContinuationToken=response['NextContinuationToken']
                )
                for obj in sorted(response.get('Contents', []), key=lambda x: x['Key']):
                    hash_parts.append(f"{obj['Key']}:{obj['ETag']}:{obj['LastModified'].isoformat()}")

            # Also include the mapping file hash
            try:
                mapping_response = self.s3_client.head_object(
                    Bucket=self.bucket_name,
                    Key="config/brand_product_mapping.json"
                )
                hash_parts.append(f"mapping:{mapping_response['ETag']}")
            except ClientError:
                hash_parts.append("mapping:none")

            # Create combined hash
            combined = "|".join(hash_parts)
            return hashlib.md5(combined.encode()).hexdigest()

        except Exception as e:
            print(f"Error computing data hash: {e}")
            return ""

    def load_all_data_from_s3(self, processor) -> dict:
        """
        Load all uploaded data from S3 and return merged DataFrames.

        Args:
            processor: DataProcessor instance for cleaning data

        Returns:
            Dict with 'sales', 'brand', 'product', 'customer', 'invoice' DataFrames (or None if no data)
        """
        if not self.is_configured():
            return {'sales': None, 'brand': None, 'product': None, 'customer': None, 'invoice': None}

        result = {'sales': None, 'brand': None, 'product': None, 'customer': None, 'invoice': None}
        
        try:
            # List all files in raw-uploads
            files = self.list_files(prefix="raw-uploads/")
            
            if not files:
                return result
            
            # Group files by type
            sales_files = [f for f in files if '/sales_' in f and f.endswith('.csv')]
            brand_files = [f for f in files if '/brand_' in f and f.endswith('.csv')]
            product_files = [f for f in files if '/product_' in f and f.endswith('.csv')]
            customer_files = [f for f in files if '/customers_' in f and f.endswith('.csv')]
            invoice_files = [f for f in files if '/invoices_' in f and f.endswith('.csv')]
            
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

            # Load and merge customer data
            if customer_files:
                customer_dfs = []
                for f in customer_files:
                    try:
                        df = self.download_file(f)
                        if df is not None and not df.empty:
                            store_id = self._extract_store_from_path(f)

                            df = processor.clean_customer_data(df)
                            df['Upload_Store'] = store_id
                            df['Upload_Date'] = pd.to_datetime(datetime.now())

                            customer_dfs.append(df)
                    except Exception as e:
                        print(f"Error loading {f}: {e}")
                        continue

                if customer_dfs:
                    result['customer'] = pd.concat(customer_dfs, ignore_index=True)
                    # Remove duplicates based on Customer ID, keeping latest
                    customer_id_col = 'Customer ID' if 'Customer ID' in result['customer'].columns else 'id'
                    if customer_id_col in result['customer'].columns:
                        result['customer'] = result['customer'].drop_duplicates(
                            subset=[customer_id_col],
                            keep='last'
                        )

            # Load and merge invoice data
            if invoice_files:
                invoice_dfs = []
                for f in invoice_files:
                    try:
                        df = self.download_file(f)
                        if df is not None and not df.empty:
                            store_id = self._extract_store_from_path(f)
                            date_range = self._extract_date_range_from_path(f)

                            df = processor.clean_invoice_data(df)
                            df['Upload_Store'] = store_id

                            if date_range:
                                df['Upload_Start_Date'] = pd.to_datetime(date_range[0])
                                df['Upload_End_Date'] = pd.to_datetime(date_range[1])

                            invoice_dfs.append(df)
                    except Exception as e:
                        print(f"Error loading {f}: {e}")
                        continue

                if invoice_dfs:
                    result['invoice'] = pd.concat(invoice_dfs, ignore_index=True)
                    # Remove duplicates
                    if 'Upload_Start_Date' in result['invoice'].columns and 'Invoice Number' in result['invoice'].columns:
                        result['invoice'] = result['invoice'].drop_duplicates(
                            subset=['Invoice Number', 'Upload_Store', 'Upload_Start_Date'],
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

        # Handle column name change: Treez renamed 'Brand' to 'Product Brand' after 12/01/2025
        if 'Product Brand' in df.columns and 'Brand' not in df.columns:
            df = df.rename(columns={'Product Brand': 'Brand'})

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

    @staticmethod
    def clean_customer_data(df: pd.DataFrame) -> pd.DataFrame:
        """Clean and process customer data."""
        df = df.copy()

        # Remove BOM if present
        df.columns = [col.strip().replace('\ufeff', '') for col in df.columns]

        # Extract store identifier
        df['Store_ID'] = df['Store Name'].apply(lambda x:
            'grass_roots' if 'Grass Roots' in str(x) else 'barbary_coast'
        )

        # Convert date columns
        date_cols = ['Date of Birth', 'Customer Drivers License Expiration Date',
                     'Sign-Up Date', 'Last Visit Date', 'First Purchase Date',
                     'Customer Medical Id Expiration Date']

        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # Calculate age from date of birth
        df['Age'] = (datetime.now() - df['Date of Birth']).dt.days // 365

        # Calculate days since last visit
        df['Days Since Last Visit'] = (datetime.now() - df['Last Visit Date']).dt.days

        # Calculate customer lifetime (days since sign-up)
        df['Customer Lifetime Days'] = (datetime.now() - df['Sign-Up Date']).dt.days

        # Convert numeric columns
        numeric_cols = ['Lifetime In-Store Visits', 'Lifetime Transactions',
                       'Lifetime Net Sales', 'Lifetime Gross Receipts',
                       'Lifetime Discounts', 'Lifetime Avg Order Value',
                       'Rewards Points Balance', 'Reward Points ($) Balance']

        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Create customer segments based on lifetime value
        df['Customer Segment'] = pd.cut(
            df['Lifetime Net Sales'],
            bins=[-1, 500, 2000, 5000, 10000, float('inf')],
            labels=['New/Low', 'Regular', 'Good', 'VIP', 'Whale']
        )

        # Create recency segments (days since last visit)
        df['Recency Segment'] = pd.cut(
            df['Days Since Last Visit'],
            bins=[-1, 30, 90, 180, 365, float('inf')],
            labels=['Active', 'Warm', 'Cool', 'Cold', 'Lost']
        )

        # Parse customer groups
        df['Customer Group(s)'] = df['Customer Group(s)'].fillna('')

        return df

    @staticmethod
    def clean_invoice_data(df: pd.DataFrame) -> pd.DataFrame:
        """Clean and process invoice/purchase order data."""
        df = df.copy()

        # Remove BOM if present
        df.columns = [col.strip().replace('\ufeff', '') for col in df.columns]

        # Convert date columns if present
        date_cols = ['Invoice Date', 'Order Date', 'Delivery Date', 'Due Date']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # Convert numeric columns if present
        numeric_cols = ['Total Amount', 'Subtotal', 'Tax', 'Shipping', 'Discount',
                       'Quantity', 'Unit Price', 'Line Total']
        for col in numeric_cols:
            if col in df.columns:
                # Remove currency symbols and commas
                if df[col].dtype == 'object':
                    df[col] = df[col].str.replace('$', '').str.replace(',', '')
                df[col] = pd.to_numeric(df[col], errors='coerce')

        return df


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

    # Filter out invalid data points (zero, null, or suspiciously low values)
    df = df[
        (df['Net Sales'].notna()) &
        (df['Net Sales'] > 100) &  # Filter out zero/near-zero sales days
        (df['Tickets Count'].notna()) &
        (df['Tickets Count'] > 10)  # Filter out days with almost no transactions
    ].copy()

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
    # Filter out invalid data
    df = df[
        (df['Net Sales'].notna()) &
        (df['Net Sales'] > 0) &
        (df['Gross Margin %'].notna())
    ]
    top_brands = df.nlargest(top_n, 'Net Sales').copy()

    # Handle margin percentage - check if already in percentage form or decimal
    max_margin = top_brands['Gross Margin %'].max() if len(top_brands) > 0 else 0
    if max_margin <= 1:
        # Decimal form (0.55), convert to percentage
        top_brands['Margin_Pct'] = top_brands['Gross Margin %'] * 100
    else:
        # Already percentage form (55)
        top_brands['Margin_Pct'] = top_brands['Gross Margin %']
    
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
# INVOICE DATA LOADING FROM DYNAMODB
# =============================================================================

def load_invoice_data_from_dynamodb(invoice_service):
    """
    Load invoice line items from DynamoDB and convert to pandas DataFrame.

    Args:
        invoice_service: InvoiceDataService instance

    Returns:
        pd.DataFrame with invoice line items, or None if loading fails
    """
    try:
        from decimal import Decimal

        # Get the line items table
        line_items_table = invoice_service.dynamodb.Table(invoice_service.line_items_table_name)

        # Scan all line items
        response = line_items_table.scan()
        items = response.get('Items', [])

        # Handle pagination
        while 'LastEvaluatedKey' in response:
            response = line_items_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response.get('Items', []))

        if not items:
            return None

        # Convert DynamoDB items to DataFrame
        # Convert Decimal to float for pandas compatibility
        records = []
        for item in items:
            record = {}
            for key, value in item.items():
                if isinstance(value, Decimal):
                    record[key] = float(value)
                else:
                    record[key] = value
            records.append(record)

        df = pd.DataFrame(records)

        # Rename columns to match expected format for analytics
        # Map DynamoDB schema to app's expected invoice data schema
        column_mapping = {
            'invoice_id': 'Invoice Number',
            'invoice_date': 'Invoice Date',
            'download_date': 'Download Date',
            'brand': 'Brand',
            'product_name': 'Product',
            'product_type': 'Product Type',
            'product_subtype': 'Product Subtype',
            'sku_units': 'Units',
            'unit_cost': 'Unit Cost',
            'total_cost': 'Total Cost',
            'total_cost_with_excise': 'Total Cost With Excise',
            'trace_id': 'Trace ID',
            'strain': 'Strain',
            'unit_size': 'Unit Size',
            'is_promo': 'Is Promo'
        }

        # Rename columns that exist
        for old_name, new_name in column_mapping.items():
            if old_name in df.columns:
                df.rename(columns={old_name: new_name}, inplace=True)

        # Convert date columns to datetime
        if 'Invoice Date' in df.columns:
            df['Invoice Date'] = pd.to_datetime(df['Invoice Date'], errors='coerce')
        if 'Download Date' in df.columns:
            df['Download Date'] = pd.to_datetime(df['Download Date'], errors='coerce')

        # Add a source column to distinguish DynamoDB data from S3 data
        df['Data Source'] = 'DynamoDB'

        return df

    except Exception as e:
        print(f"Error loading invoice data from DynamoDB: {e}")
        return None


# =============================================================================
# CACHED DATA LOADING WITH HASH VALIDATION
# =============================================================================

@st.cache_data(ttl=86400, show_spinner=False)  # 24-hour TTL for hash check (use manual refresh for immediate updates)
def _get_s3_data_hash(bucket_name: str, _s3_manager) -> str:
    """Get the current hash of S3 data (lightweight metadata check)."""
    return _s3_manager.get_data_hash()


@st.cache_data(ttl=86400, show_spinner=False)  # 24-hour TTL for DynamoDB hash check (use manual refresh for immediate updates)
def _get_dynamodb_hash(_aws_access_key: str, _aws_secret_key: str, _region: str) -> str:
    """Get a hash based on DynamoDB invoice count (lightweight check)."""
    try:
        from invoice_extraction import InvoiceDataService
        invoice_service = InvoiceDataService(
            aws_access_key=_aws_access_key,
            aws_secret_key=_aws_secret_key,
            region=_region
        )
        # Get item count from table (lightweight operation)
        table = invoice_service.dynamodb.Table(invoice_service.line_items_table_name)
        # Use describe_table for item count estimate (fast)
        response = invoice_service.dynamodb.meta.client.describe_table(
            TableName=invoice_service.line_items_table_name
        )
        item_count = response['Table'].get('ItemCount', 0)
        last_update = response['Table'].get('TableStatus', 'ACTIVE')
        return hashlib.md5(f"{item_count}:{last_update}".encode()).hexdigest()
    except Exception as e:
        return ""


def _get_cached_s3_data(data_hash: str, s3_manager, processor) -> dict:
    """
    Load S3 data with caching. The data_hash parameter ensures cache invalidation
    when data changes. Streamlit's cache_data will return cached result if
    the hash hasn't changed.
    """
    @st.cache_data(ttl=86400, show_spinner=False)  # Cache for 24 hours (use manual refresh for immediate updates)
    def _load_s3_data(_hash: str) -> dict:
        return s3_manager.load_all_data_from_s3(processor)

    return _load_s3_data(data_hash)


def _get_cached_dynamodb_data(data_hash: str, invoice_service) -> tuple:
    """
    Load DynamoDB invoice data with caching. Hash parameter ensures
    cache invalidation when new invoices are added.
    """
    @st.cache_data(ttl=86400, show_spinner=False)  # Cache for 24 hours (use manual refresh for immediate updates)
    def _load_dynamo_data(_hash: str) -> pd.DataFrame:
        return load_invoice_data_from_dynamodb(invoice_service)

    return _load_dynamo_data(data_hash)


def _get_cached_brand_mapping(data_hash: str, s3_manager) -> dict:
    """Load brand mapping with cache invalidation based on S3 hash."""
    @st.cache_data(ttl=86400, show_spinner=False)  # Cache for 24 hours
    def _load_mapping(_hash: str) -> dict:
        return s3_manager.load_brand_product_mapping()

    return _load_mapping(data_hash)


def _clear_all_data_caches():
    """
    Clear all data caches across the application.
    Call this function when user requests a manual refresh to ensure data consistency.
    This ensures document counts, research data, and all other cached data stays in sync.
    """
    # Clear hash check caches (always defined at module level)
    try:
        _get_s3_data_hash.clear()
    except:
        pass
    try:
        _get_dynamodb_hash.clear()
    except:
        pass

    # Clear data fetch caches (defined later in the file, but available at runtime)
    try:
        _fetch_invoice_data_cached.clear()
    except:
        pass
    try:
        _fetch_research_data_cached.clear()
    except:
        pass
    try:
        _fetch_seo_data_cached.clear()
    except:
        pass
    try:
        _fetch_research_documents_cached.clear()
    except:
        pass

    # Reset session state for hash tracking to force fresh data loads
    if 'last_s3_hash' in st.session_state:
        st.session_state.last_s3_hash = None
    if 'last_dynamo_hash' in st.session_state:
        st.session_state.last_dynamo_hash = None
    if 'recommendations_data_loaded' in st.session_state:
        st.session_state.recommendations_data_loaded = False
    if 'recommendations_data_hash' in st.session_state:
        st.session_state.recommendations_data_hash = None


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
    if 'customer_data' not in st.session_state:
        st.session_state.customer_data = None
    if 'invoice_data' not in st.session_state:
        st.session_state.invoice_data = None
    if 'budtender_data' not in st.session_state:
        st.session_state.budtender_data = None
    if 'brand_product_mapping' not in st.session_state:
        st.session_state.brand_product_mapping = None

    # Track data hashes for cache invalidation
    if 'last_s3_hash' not in st.session_state:
        st.session_state.last_s3_hash = None
    if 'last_dynamo_hash' not in st.session_state:
        st.session_state.last_dynamo_hash = None

    # Track DynamoDB invoice loading status
    if 'dynamo_invoice_count' not in st.session_state:
        st.session_state.dynamo_invoice_count = 0
    if 'dynamo_load_error' not in st.session_state:
        st.session_state.dynamo_load_error = None

    # Hash-based data loading with caching
    # Step 1: Get current data hashes (lightweight metadata operations)
    current_s3_hash = _get_s3_data_hash(s3_manager.bucket_name, s3_manager) if s3_manager.is_configured() else ""

    # Get DynamoDB hash
    current_dynamo_hash = ""
    try:
        aws_access_key = st.secrets['aws']['access_key_id']
        aws_secret_key = st.secrets['aws']['secret_access_key']
        aws_region = st.secrets['aws']['region']
        current_dynamo_hash = _get_dynamodb_hash(aws_access_key, aws_secret_key, aws_region)
    except Exception:
        pass

    # Step 2: Check if data needs refresh (hash changed or first load)
    s3_needs_refresh = current_s3_hash != st.session_state.last_s3_hash
    dynamo_needs_refresh = current_dynamo_hash != st.session_state.last_dynamo_hash

    if s3_needs_refresh or dynamo_needs_refresh:
        refresh_type = []
        if s3_needs_refresh and current_s3_hash:
            refresh_type.append("S3")
        if dynamo_needs_refresh and current_dynamo_hash:
            refresh_type.append("DynamoDB")

        spinner_msg = "🔄 Loading data..." if not st.session_state.last_s3_hash else f"🔄 Refreshing data ({', '.join(refresh_type)} changed)..."

        with st.spinner(spinner_msg):
            # Load S3 data if hash changed
            if s3_needs_refresh and current_s3_hash:
                # Load brand-product mapping (cached by hash)
                st.session_state.brand_product_mapping = _get_cached_brand_mapping(current_s3_hash, s3_manager)

                # Load all CSV data from S3 (cached by hash)
                loaded_data = _get_cached_s3_data(current_s3_hash, s3_manager, processor)

                if loaded_data['sales'] is not None:
                    st.session_state.sales_data = loaded_data['sales']
                if loaded_data['brand'] is not None:
                    st.session_state.brand_data = loaded_data['brand']
                if loaded_data['product'] is not None:
                    st.session_state.product_data = loaded_data['product']
                if loaded_data['customer'] is not None:
                    st.session_state.customer_data = loaded_data['customer']
                if loaded_data['invoice'] is not None:
                    st.session_state.invoice_data = loaded_data['invoice']

                st.session_state.last_s3_hash = current_s3_hash

            # Load DynamoDB invoice data if hash changed
            if dynamo_needs_refresh and current_dynamo_hash:
                try:
                    from invoice_extraction import InvoiceDataService
                    invoice_service = InvoiceDataService(
                        aws_access_key=aws_access_key,
                        aws_secret_key=aws_secret_key,
                        region=aws_region
                    )

                    # Verify table exists
                    try:
                        invoice_service.dynamodb.Table(invoice_service.line_items_table_name).table_status
                    except Exception as table_err:
                        raise Exception(f"DynamoDB table not accessible: {invoice_service.line_items_table_name}")

                    # Load invoice data (cached by hash)
                    dynamo_invoice_df = _get_cached_dynamodb_data(current_dynamo_hash, invoice_service)
                    if dynamo_invoice_df is not None and len(dynamo_invoice_df) > 0:
                        st.session_state.dynamo_invoice_count = len(dynamo_invoice_df)
                        # Merge with existing S3 invoice data if any
                        if st.session_state.invoice_data is not None:
                            st.session_state.invoice_data = pd.concat([
                                st.session_state.invoice_data,
                                dynamo_invoice_df
                            ], ignore_index=True).drop_duplicates()
                        else:
                            st.session_state.invoice_data = dynamo_invoice_df
                    else:
                        st.session_state.dynamo_invoice_count = 0

                    st.session_state.last_dynamo_hash = current_dynamo_hash
                    st.session_state.dynamo_load_error = None

                except Exception as e:
                    st.session_state.dynamo_load_error = str(e)
                    st.session_state.dynamo_invoice_count = 0

            # Show what was loaded/refreshed
            loaded_items = []
            if st.session_state.sales_data is not None:
                loaded_items.append(f"Sales ({len(st.session_state.sales_data)} records)")
            if st.session_state.brand_data is not None:
                loaded_items.append(f"Brands ({len(st.session_state.brand_data)} records)")
            if st.session_state.product_data is not None:
                loaded_items.append(f"Products ({len(st.session_state.product_data)} records)")
            if st.session_state.customer_data is not None:
                loaded_items.append(f"Customers ({len(st.session_state.customer_data)} records)")
            if st.session_state.invoice_data is not None:
                invoice_source = ""
                if st.session_state.dynamo_invoice_count > 0:
                    invoice_source = f" ({st.session_state.dynamo_invoice_count} from DynamoDB)"
                loaded_items.append(f"Invoices ({len(st.session_state.invoice_data)} records{invoice_source})")
            if st.session_state.brand_product_mapping:
                loaded_items.append(f"Mappings ({len(st.session_state.brand_product_mapping)} brands)")

            if loaded_items:
                cache_status = "cached" if st.session_state.last_s3_hash else "loaded"
                st.toast(f"✅ Data {cache_status}: {', '.join(loaded_items)}", icon="📊")

            if st.session_state.dynamo_load_error:
                st.toast(f"⚠️ DynamoDB Error: {st.session_state.dynamo_load_error[:100]}", icon="⚠️")
    
    # Sidebar
    with st.sidebar:
        st.image("https://barbarycoastsf.com/wp-content/uploads/2025/12/icon-1.png", width=150)
        st.markdown(f"**Logged in as:** {st.session_state.get('logged_in_user', 'Unknown')}")
        st.markdown("---")
        
        # Navigation
        nav_options = [
            "📊 Dashboard",
            "📈 Sales Analytics",
            "💡 Recommendations",
        ]

        nav_options.append("🗄️ Data Center")

        # Handle navigation override from dashboard buttons
        if 'nav_override' in st.session_state:
            page = st.session_state['nav_override']
            del st.session_state['nav_override']
        else:
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

    elif page == "📈 Sales Analytics":
        render_sales_analysis(st.session_state, analytics, selected_store, date_range)

    elif page == "💡 Recommendations":
        render_recommendations(st.session_state, analytics)

    elif page == "🗄️ Data Center":
        render_data_center(s3_manager, processor)


def render_dashboard(state, analytics, store_filter):
    """Render main dashboard overview."""
    st.header("Overview Dashboard")
    
    if state.sales_data is None:
        st.info("👆 Upload your data files using the 'Data Center' page to get started.")
        
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

    st.markdown("---")

    # SEO & Industry Research Preview
    st.subheader("📈 Business Intelligence Preview")

    col1, col2 = st.columns(2)

    # SEO Preview
    with col1:
        st.markdown("### 🔍 SEO Status")
        if SEO_AVAILABLE:
            try:
                # SEOFindingsViewer already imported from dashboard package

                # Try to load latest SEO summary
                viewer = SEOFindingsViewer(website="https://barbarycoastsf.com")
                summary = viewer.load_latest_summary()

                if summary:
                    score = summary.get("overall_score", 0)

                    # Display score with color
                    if score >= 70:
                        st.success(f"**Overall Score: {score}/100** ✅")
                    elif score >= 50:
                        st.warning(f"**Overall Score: {score}/100** ⚠️")
                    else:
                        st.error(f"**Overall Score: {score}/100** ❌")

                    # Top priority
                    priorities = summary.get("top_priorities", [])
                    if priorities:
                        st.markdown("**Top Priority:**")
                        st.markdown(f"- {priorities[0].get('priority', 'N/A')[:80]}...")

                    # Quick wins
                    wins = summary.get("quick_wins", [])
                    if wins:
                        st.markdown("**Quick Win:**")
                        win_text = wins[0] if isinstance(wins[0], str) else wins[0].get('action', 'N/A')
                        st.markdown(f"⚡ {win_text[:80]}...")

                    st.markdown(f"*Last analyzed: {summary.get('analyzed_at', 'Unknown')[:10]}*")

                    if st.button("View Full SEO Analysis →", key="seo_button"):
                        st.session_state['nav_override'] = "🔍 SEO Analysis"
                        st.rerun()
                else:
                    st.info("No SEO data available yet. Run an SEO analysis to get started.")
            except Exception as e:
                st.info("SEO module available. Click to view analysis.")
        else:
            st.info("SEO module not installed.")

    # Industry Research Preview
    with col2:
        st.markdown("### 🔬 Industry Insights")
        if RESEARCH_AVAILABLE:
            try:
                # MonthlyResearchSummarizer already imported from dashboard package

                # Get API key
                api_key = os.environ.get("ANTHROPIC_API_KEY")
                if not api_key:
                    try:
                        api_key = st.secrets.get("ANTHROPIC_API_KEY") or st.secrets.get("anthropic", {}).get("ANTHROPIC_API_KEY")
                    except:
                        pass

                if api_key:
                    summarizer = MonthlyResearchSummarizer(api_key)

                    # Load most recent summary
                    recent_summary = summarizer.recall_summary()

                    if recent_summary:
                        month_name = recent_summary.get('month_name', 'Recent')
                        docs = recent_summary.get('documents_analyzed', 0)

                        st.markdown(f"**Latest: {month_name}**")
                        st.metric("Documents Analyzed", docs)

                        # Top insight
                        insights = recent_summary.get('key_insights', [])
                        if insights:
                            top_insight = insights[0]
                            importance = top_insight.get('importance', 'medium')
                            emoji = {"high": "🔴", "medium": "🟡", "low": "🟢"}.get(importance, "⚪")
                            st.markdown(f"{emoji} **Top Insight:**")
                            st.markdown(f"{top_insight.get('insight', 'N/A')[:100]}...")

                        if st.button("View Full Research →", key="research_button"):
                            st.session_state['nav_override'] = "🔬 Industry Research"
                            st.rerun()
                    else:
                        st.info("No industry research available yet. Upload documents to get started.")
                else:
                    st.info("Configure ANTHROPIC_API_KEY to view research.")
            except Exception as e:
                st.info("Research module available. Click to view insights.")
        else:
            st.info("Research module not installed.")


def render_sales_analysis(state, analytics, store_filter, date_filter=None):
    """Render comprehensive sales analysis page with all sales-related insights."""
    st.header("Sales Analytics")

    if state.sales_data is None:
        st.warning("Please upload sales data first.")
        return

    # Tabs for different sales views (including Customer Analytics and Budtender Analytics)
    tab1, tab2, tab3, tab4, tab5, tab_customers, tab_budtenders = st.tabs([
        "📈 Sales Trends",
        "🏷️ Brand Performance",
        "📦 Product Categories",
        "📊 Daily Breakdown",
        "🔍 Raw Data",
        "👥 Customer Analytics",
        "🎯 Budtender Analytics"
    ])
    
    # ===== TAB 1: Sales Trends =====
    with tab1:
        df = state.sales_data.copy()

        # Apply store filter
        if store_filter != "All Stores":
            store_id = [k for k, v in STORE_DISPLAY_NAMES.items() if v == store_filter]
            if store_id:
                df = df[df['Store_ID'] == store_id[0]]

        # Filter out invalid data points (zero, null, or suspiciously low values)
        df = df[
            (df['Net Sales'].notna()) &
            (df['Net Sales'] > 100) &
            (df['Customers Count'].notna()) &
            (df['Customers Count'] > 5)
        ].copy()

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

    # ===== TAB 2: Brand Performance =====
    with tab2:
        if state.brand_data is None:
            st.warning("Please upload brand data to view brand performance.")
        else:
            df_brand = state.brand_data.copy()

            # Show available date ranges in the data
            if 'Upload_Start_Date' in df_brand.columns and 'Upload_End_Date' in df_brand.columns:
                date_ranges = df_brand.groupby(['Upload_Start_Date', 'Upload_End_Date', 'Upload_Store']).size().reset_index(name='records')

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
                    df_brand = df_brand[
                        (df_brand['Upload_Start_Date'] <= filter_end) &
                        (df_brand['Upload_End_Date'] >= filter_start)
                    ]

                    if len(df_brand) == 0:
                        st.warning(f"No brand data available for the selected date range ({filter_start.strftime('%m/%d/%Y')} - {filter_end.strftime('%m/%d/%Y')})")
                    else:
                        st.info(f"📅 Showing data for: {filter_start.strftime('%m/%d/%Y')} - {filter_end.strftime('%m/%d/%Y')}")

            # Filter by store if specified
            if store_filter and store_filter != 'All Stores':
                store_id = [k for k, v in STORE_DISPLAY_NAMES.items() if v == store_filter]
                if store_id and 'Upload_Store' in df_brand.columns:
                    df_brand = df_brand[df_brand['Upload_Store'] == store_id[0]]

            if len(df_brand) > 0:
                # Top performers
                st.subheader("Top Performing Brands")
                top_n = st.slider("Number of brands to show", 10, 50, 20, key="brand_top_n")

                fig = plot_brand_performance(df_brand, top_n)
                st.plotly_chart(fig, use_container_width=True)

                # Brand table
                col1, col2 = st.columns(2)

                with col1:
                    st.subheader("🏆 Top Brands by Revenue")
                    top_brands = analytics.identify_top_brands(df_brand, 10, store_filter)
                    st.dataframe(top_brands, width='stretch')

                with col2:
                    st.subheader("⚠️ Low Margin Brands")
                    underperformers = analytics.identify_underperformers(df_brand)
                    st.dataframe(underperformers, width='stretch')

                # Margin vs Sales scatter
                st.subheader("Margin vs. Sales Analysis")

                # Filter to significant brands with valid margin data
                significant_brands = df_brand[
                    (df_brand['Net Sales'] > 1000) &  # Lowered threshold
                    (df_brand['Gross Margin %'].notna()) &
                    (df_brand['Gross Margin %'] > 0)
                ].copy()

                # Handle margin percentage - check if already in percentage form or decimal
                # If max value > 1, it's already a percentage; if <= 1, it's a decimal
                if len(significant_brands) > 0:
                    max_margin = significant_brands['Gross Margin %'].max()
                    if max_margin <= 1:
                        # Decimal form (0.55), convert to percentage
                        significant_brands['Margin_Pct'] = significant_brands['Gross Margin %'] * 100
                    else:
                        # Already percentage form (55)
                        significant_brands['Margin_Pct'] = significant_brands['Gross Margin %']

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
                else:
                    st.info("No brand data with sufficient sales volume to display.")

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

    # ===== TAB 3: Product Categories =====
    with tab3:
        if state.product_data is None:
            st.warning("Please upload product data to view category analysis.")
        else:
            df_product = state.product_data.copy()

            # Show available date ranges in the data
            if 'Upload_Start_Date' in df_product.columns and 'Upload_End_Date' in df_product.columns:
                date_ranges = df_product.groupby(['Upload_Start_Date', 'Upload_End_Date', 'Upload_Store']).size().reset_index(name='records')

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
                    df_product = df_product[
                        (df_product['Upload_Start_Date'] <= filter_end) &
                        (df_product['Upload_End_Date'] >= filter_start)
                    ]

                    if len(df_product) == 0:
                        st.warning(f"No product data available for the selected date range ({filter_start.strftime('%m/%d/%Y')} - {filter_end.strftime('%m/%d/%Y')})")
                    else:
                        st.info(f"📅 Showing data for: {filter_start.strftime('%m/%d/%Y')} - {filter_end.strftime('%m/%d/%Y')}")

            # Filter by store if specified
            if store_filter and store_filter != 'All Stores':
                store_id = [k for k, v in STORE_DISPLAY_NAMES.items() if v == store_filter]
                if store_id and 'Upload_Store' in df_product.columns:
                    df_product = df_product[df_product['Upload_Store'] == store_id[0]]

            if len(df_product) > 0:
                col1, col2 = st.columns(2)

                with col1:
                    fig = plot_category_breakdown(df_product)
                    st.plotly_chart(fig, use_container_width=True)

                with col2:
                    st.subheader("Category Details")
                    df_product['Sales Share %'] = (df_product['Net Sales'] / df_product['Net Sales'].sum() * 100).round(2)
                    st.dataframe(df_product, width='stretch')

                # Category bar chart
                fig = px.bar(df_product, x='Product Type', y='Net Sales',
                            title='Net Sales by Product Category',
                            color='Net Sales',
                            color_continuous_scale='Blues')
                st.plotly_chart(fig, use_container_width=True)

    # ===== TAB 4: Daily Breakdown =====
    with tab4:
        df = state.sales_data.copy()

        # Apply store filter
        if store_filter != "All Stores":
            store_id = [k for k, v in STORE_DISPLAY_NAMES.items() if v == store_filter]
            if store_id:
                df = df[df['Store_ID'] == store_id[0]]

        # Day of week analysis
        df['Day_of_Week'] = df['Date'].dt.day_name()
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

        dow_sales = df.groupby(['Day_of_Week', 'Store_ID'])['Net Sales'].mean().reset_index()
        dow_sales['Day_of_Week'] = pd.Categorical(dow_sales['Day_of_Week'], categories=day_order, ordered=True)
        dow_sales = dow_sales.sort_values('Day_of_Week')

        fig = px.bar(dow_sales, x='Day_of_Week', y='Net Sales', color='Store_ID',
                    barmode='group', title='Average Sales by Day of Week')
        st.plotly_chart(fig, use_container_width=True)

    # ===== TAB 5: Raw Data =====
    with tab5:
        df = state.sales_data.copy()

        # Apply store filter
        if store_filter != "All Stores":
            store_id = [k for k, v in STORE_DISPLAY_NAMES.items() if v == store_filter]
            if store_id:
                df = df[df['Store_ID'] == store_id[0]]

        st.dataframe(df.sort_values('Date', ascending=False), width='stretch')

        # Download button
        csv = df.to_csv(index=False)
        st.download_button("📥 Download Data", csv, "sales_data.csv", "text/csv")

    # ===== TAB 6: Customer Analytics =====
    with tab_customers:
        _render_customer_analytics_content(state, analytics, store_filter, date_filter)

    # ===== TAB 7: Budtender Analytics =====
    with tab_budtenders:
        _render_budtender_analytics(state, analytics, store_filter)


def _render_budtender_analytics(state, analytics, store_filter):
    """Render comprehensive budtender performance analytics."""

    if state.budtender_data is None:
        st.warning("No budtender data loaded. Please upload Budtender Performance files in the Data Center.")
        st.info("Go to **Data Center → Budtender Performance** to upload your Treez BudtenderPerformanceLifetime reports.")
        return

    df = state.budtender_data.copy()

    # Standardize column names (handle both formats)
    col_mapping = {
        'Product Brand': 'Product_Brand',
        'Units Sold': 'Units_Sold',
        'Net Sales': 'Net_Sales',
        'Gross  Margin ': 'Gross_Margin',
        'Discount %': 'Discount_Pct',
        'Store Name': 'Store_Name'
    }
    for old, new in col_mapping.items():
        if old in df.columns and new not in df.columns:
            df[new] = df[old]

    # Apply store filter first
    if store_filter != "All Stores":
        store_id = 'barbary_coast' if store_filter == "Barbary Coast" else 'grass_roots'
        if 'Store_ID' in df.columns:
            df = df[df['Store_ID'] == store_id]

    # =========================================================================
    # BUDTENDER FILTER - Allow user to select/deselect specific budtenders
    # =========================================================================
    all_budtenders = sorted(df['Employee'].unique().tolist())

    # Calculate sales for each budtender for the filter display
    budtender_sales = df.groupby('Employee')['Net_Sales'].sum().sort_values(ascending=False)

    with st.expander("🎯 **Filter by Budtender** (click to expand)", expanded=False):
        st.caption("Select which budtenders to include in the analysis. By default, all budtenders are selected.")

        # Quick action buttons
        filter_col1, filter_col2, filter_col3, filter_col4 = st.columns(4)

        with filter_col1:
            if st.button("Select All", key="budtender_select_all"):
                st.session_state.selected_budtenders_filter = all_budtenders
                st.rerun()

        with filter_col2:
            if st.button("Clear All", key="budtender_clear_all"):
                st.session_state.selected_budtenders_filter = []
                st.rerun()

        with filter_col3:
            if st.button("Top 10", key="budtender_top_10"):
                st.session_state.selected_budtenders_filter = budtender_sales.head(10).index.tolist()
                st.rerun()

        with filter_col4:
            if st.button("Top 25", key="budtender_top_25"):
                st.session_state.selected_budtenders_filter = budtender_sales.head(25).index.tolist()
                st.rerun()

        # Initialize session state for selected budtenders if not exists
        if 'selected_budtenders_filter' not in st.session_state:
            st.session_state.selected_budtenders_filter = all_budtenders

        # Text search for budtenders
        st.markdown("---")
        search_col1, search_col2 = st.columns([3, 1])
        with search_col1:
            budtender_search = st.text_input(
                "🔍 Search budtenders by name",
                placeholder="Type a name to filter...",
                key="budtender_search_input",
                help="Type part of a budtender's name to filter the list"
            )
        with search_col2:
            if st.button("Select Matches", key="budtender_select_matches", disabled=not budtender_search):
                # Find budtenders matching the search term (case-insensitive)
                matching = [b for b in all_budtenders if budtender_search.lower() in b.lower()]
                if matching:
                    st.session_state.selected_budtenders_filter = matching
                    st.rerun()

        # Show search results preview
        if budtender_search:
            matching_budtenders = [b for b in all_budtenders if budtender_search.lower() in b.lower()]
            if matching_budtenders:
                st.caption(f"Found **{len(matching_budtenders)}** matching budtender(s): {', '.join(matching_budtenders[:5])}{'...' if len(matching_budtenders) > 5 else ''}")
            else:
                st.caption("No budtenders match your search.")
        st.markdown("---")

        # Ensure selected budtenders are valid (in case data changed)
        valid_selected = [b for b in st.session_state.selected_budtenders_filter if b in all_budtenders]

        # Create options with sales info for better context
        budtender_options = []
        for budtender in all_budtenders:
            sales = budtender_sales.get(budtender, 0)
            budtender_options.append(f"{budtender} (${sales:,.0f})")

        # Map display names back to actual names
        display_to_actual = {f"{b} (${budtender_sales.get(b, 0):,.0f})": b for b in all_budtenders}

        # Multi-select with checkboxes
        selected_display = st.multiselect(
            "Select Budtenders",
            options=budtender_options,
            default=[f"{b} (${budtender_sales.get(b, 0):,.0f})" for b in valid_selected],
            key="budtender_multiselect_filter",
            help="Select one or more budtenders to filter the analysis"
        )

        # Convert display names back to actual budtender names
        selected_budtenders = [display_to_actual[d] for d in selected_display]

        # Update session state
        st.session_state.selected_budtenders_filter = selected_budtenders

        # Show selection summary
        if len(selected_budtenders) < len(all_budtenders):
            st.info(f"**{len(selected_budtenders)}** of **{len(all_budtenders)}** budtenders selected")

    # Apply budtender filter
    if 'selected_budtenders_filter' in st.session_state and st.session_state.selected_budtenders_filter:
        df = df[df['Employee'].isin(st.session_state.selected_budtenders_filter)]
    elif 'selected_budtenders_filter' in st.session_state and len(st.session_state.selected_budtenders_filter) == 0:
        st.warning("No budtenders selected. Please select at least one budtender from the filter above.")
        return

    # Show current filter status
    active_budtenders = df['Employee'].nunique()
    total_budtenders = len(all_budtenders)

    if active_budtenders < total_budtenders:
        st.info(f"Analyzing **{len(df):,}** records | **{active_budtenders}** of **{total_budtenders}** budtenders selected")
    else:
        st.info(f"Analyzing **{len(df):,}** performance records across **{active_budtenders}** budtenders")

    # Create subtabs for different analytics views
    bud_tab1, bud_tab2, bud_tab3, bud_tab4, bud_tab5 = st.tabs([
        "Overview",
        "Top Performers",
        "Product Insights",
        "Brand Analysis",
        "Performance Comparison"
    ])

    # ===== Overview Tab =====
    with bud_tab1:
        st.subheader("Budtender Performance Overview")

        # Key metrics row
        metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)

        with metric_col1:
            total_budtenders = df['Employee'].nunique()
            st.metric("Total Budtenders", total_budtenders)

        with metric_col2:
            total_sales = df['Net_Sales'].sum() if 'Net_Sales' in df.columns else 0
            st.metric("Total Net Sales", f"${total_sales:,.0f}")

        with metric_col3:
            total_units = df['Units_Sold'].sum() if 'Units_Sold' in df.columns else 0
            st.metric("Total Units Sold", f"{total_units:,.0f}")

        with metric_col4:
            avg_margin = df['Gross_Margin'].mean() * 100 if 'Gross_Margin' in df.columns else 0
            st.metric("Avg Gross Margin", f"{avg_margin:.1f}%")

        st.markdown("---")

        # Sales distribution by budtender
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Sales Distribution by Budtender**")
            budtender_sales = df.groupby('Employee')['Net_Sales'].sum().sort_values(ascending=False)

            fig = go.Figure(data=[go.Bar(
                x=budtender_sales.head(15).index,
                y=budtender_sales.head(15).values,
                marker_color='#6c5ce7'
            )])
            fig.update_layout(
                height=350,
                xaxis_title="Budtender",
                yaxis_title="Net Sales ($)",
                xaxis_tickangle=-45
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown("**Units Sold Distribution**")
            budtender_units = df.groupby('Employee')['Units_Sold'].sum().sort_values(ascending=False)

            fig = go.Figure(data=[go.Bar(
                x=budtender_units.head(15).index,
                y=budtender_units.head(15).values,
                marker_color='#00b894'
            )])
            fig.update_layout(
                height=350,
                xaxis_title="Budtender",
                yaxis_title="Units Sold",
                xaxis_tickangle=-45
            )
            st.plotly_chart(fig, use_container_width=True)

        # Sales concentration analysis
        st.markdown("---")
        st.markdown("**Sales Concentration Analysis**")

        budtender_totals = df.groupby('Employee')['Net_Sales'].sum().sort_values(ascending=False)
        total_sales = budtender_totals.sum()

        # Calculate cumulative percentage
        cumsum = budtender_totals.cumsum()
        cumsum_pct = (cumsum / total_sales * 100).values

        # Find key thresholds
        top_10_pct_count = max(1, int(len(budtender_totals) * 0.1))
        top_20_pct_count = max(1, int(len(budtender_totals) * 0.2))

        top_10_contribution = budtender_totals.head(top_10_pct_count).sum() / total_sales * 100
        top_20_contribution = budtender_totals.head(top_20_pct_count).sum() / total_sales * 100

        conc_col1, conc_col2, conc_col3 = st.columns(3)
        with conc_col1:
            st.metric(f"Top 10% ({top_10_pct_count} budtenders)", f"{top_10_contribution:.1f}% of sales")
        with conc_col2:
            st.metric(f"Top 20% ({top_20_pct_count} budtenders)", f"{top_20_contribution:.1f}% of sales")
        with conc_col3:
            median_sales = budtender_totals.median()
            st.metric("Median Sales/Budtender", f"${median_sales:,.0f}")

    # ===== Top Performers Tab =====
    with bud_tab2:
        st.subheader("Top Performing Budtenders")

        # Aggregated performance table
        performance_df = df.groupby('Employee').agg({
            'Net_Sales': 'sum',
            'Units_Sold': 'sum',
            'Product_Brand': 'nunique',
            'Gross_Margin': 'mean',
            'Discount_Pct': 'mean'
        }).round(4)

        performance_df.columns = ['Total Sales', 'Units Sold', 'Brands Sold', 'Avg Margin', 'Avg Discount']
        performance_df['Avg Margin'] = (performance_df['Avg Margin'] * 100).round(1)
        performance_df['Avg Discount'] = (performance_df['Avg Discount'] * 100).round(2)
        performance_df['Avg Sale Value'] = (performance_df['Total Sales'] / performance_df['Units Sold']).round(2)
        performance_df = performance_df.sort_values('Total Sales', ascending=False)

        # Top performers selector
        top_n = st.slider("Number of budtenders to display", 10, 50, 20, key="top_budtenders_slider")

        # Display table
        display_df = performance_df.head(top_n).copy()
        display_df['Total Sales'] = display_df['Total Sales'].apply(lambda x: f"${x:,.0f}")
        display_df['Units Sold'] = display_df['Units Sold'].apply(lambda x: f"{x:,.0f}")
        display_df['Avg Sale Value'] = display_df['Avg Sale Value'].apply(lambda x: f"${x:.2f}")
        display_df['Avg Margin'] = display_df['Avg Margin'].apply(lambda x: f"{x:.1f}%")
        display_df['Avg Discount'] = display_df['Avg Discount'].apply(lambda x: f"{x:.2f}%")

        st.dataframe(display_df, use_container_width=True, height=400)

        st.markdown("---")

        # Performance scatter plot
        st.markdown("**Sales vs. Units Sold (Bubble = Brands Sold)**")

        scatter_df = performance_df.reset_index().head(30)
        fig = px.scatter(
            scatter_df,
            x='Units Sold',
            y=scatter_df['Total Sales'].str.replace('$', '').str.replace(',', '').astype(float) if isinstance(scatter_df['Total Sales'].iloc[0], str) else scatter_df['Total Sales'],
            size='Brands Sold',
            color='Avg Margin',
            hover_name='Employee',
            color_continuous_scale='RdYlGn',
            labels={'y': 'Total Sales ($)'}
        )
        fig.update_layout(height=450)
        st.plotly_chart(fig, use_container_width=True)

    # ===== Product Insights Tab =====
    with bud_tab3:
        st.subheader("Product Sales Insights")

        # Top products overall
        product_sales = df.groupby('Product_Brand').agg({
            'Units_Sold': 'sum',
            'Net_Sales': 'sum',
            'Employee': 'nunique',
            'Gross_Margin': 'mean'
        }).round(4)
        product_sales.columns = ['Units Sold', 'Net Sales', 'Budtenders Selling', 'Avg Margin']
        product_sales['Avg Margin'] = (product_sales['Avg Margin'] * 100).round(1)
        product_sales = product_sales.sort_values('Net Sales', ascending=False)

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Top 20 Products by Revenue**")
            top_products_sales = product_sales.head(20).copy()

            fig = go.Figure(data=[go.Bar(
                y=top_products_sales.index,
                x=top_products_sales['Net Sales'],
                orientation='h',
                marker_color='#e17055',
                text=[f"${x:,.0f}" for x in top_products_sales['Net Sales']],
                textposition='auto'
            )])
            fig.update_layout(height=500, yaxis={'categoryorder': 'total ascending'})
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown("**Top 20 Products by Units Sold**")
            top_products_units = product_sales.sort_values('Units Sold', ascending=False).head(20)

            fig = go.Figure(data=[go.Bar(
                y=top_products_units.index,
                x=top_products_units['Units Sold'],
                orientation='h',
                marker_color='#00cec9',
                text=[f"{x:,.0f}" for x in top_products_units['Units Sold']],
                textposition='auto'
            )])
            fig.update_layout(height=500, yaxis={'categoryorder': 'total ascending'})
            st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")

        # Product adoption by budtenders
        st.markdown("**Product Adoption Across Budtenders**")

        total_budtenders = df['Employee'].nunique()
        product_adoption = product_sales[['Budtenders Selling', 'Net Sales']].copy()
        product_adoption['Adoption Rate'] = (product_adoption['Budtenders Selling'] / total_budtenders * 100).round(1)
        product_adoption = product_adoption.sort_values('Adoption Rate', ascending=False).head(30)

        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=product_adoption.index,
            y=product_adoption['Adoption Rate'],
            name='Adoption Rate (%)',
            marker_color='#74b9ff'
        ))
        fig.update_layout(
            height=400,
            xaxis_tickangle=-45,
            yaxis_title="% of Budtenders Selling",
            showlegend=False
        )
        st.plotly_chart(fig, use_container_width=True)

        # High-margin products
        st.markdown("---")
        st.markdown("**High-Margin Products (Top 20 by Margin)**")

        high_margin = product_sales[product_sales['Net Sales'] > product_sales['Net Sales'].median()].sort_values('Avg Margin', ascending=False).head(20)

        fig = go.Figure(data=[go.Bar(
            x=high_margin.index,
            y=high_margin['Avg Margin'],
            marker_color=high_margin['Avg Margin'],
            marker_colorscale='RdYlGn',
            text=[f"{x:.1f}%" for x in high_margin['Avg Margin']],
            textposition='auto'
        )])
        fig.update_layout(height=350, xaxis_tickangle=-45, yaxis_title="Gross Margin %")
        st.plotly_chart(fig, use_container_width=True)

    # ===== Brand Analysis Tab =====
    with bud_tab4:
        st.subheader("Brand Performance Analysis")

        # Brand selector
        all_brands = sorted(df['Product_Brand'].unique().tolist())
        selected_brands = st.multiselect(
            "Select brands to analyze (leave empty for all)",
            options=all_brands,
            default=[],
            key="brand_analysis_select"
        )

        if selected_brands:
            brand_df = df[df['Product_Brand'].isin(selected_brands)]
        else:
            brand_df = df

        # Brand-level metrics
        brand_metrics = brand_df.groupby('Product_Brand').agg({
            'Net_Sales': 'sum',
            'Units_Sold': 'sum',
            'Employee': 'nunique',
            'Gross_Margin': 'mean',
            'Discount_Pct': 'mean'
        }).round(4)
        brand_metrics.columns = ['Total Sales', 'Units Sold', 'Budtenders', 'Avg Margin', 'Avg Discount']
        brand_metrics['Avg Margin'] = (brand_metrics['Avg Margin'] * 100).round(1)
        brand_metrics['Avg Discount'] = (brand_metrics['Avg Discount'] * 100).round(2)
        brand_metrics['Revenue/Unit'] = (brand_metrics['Total Sales'] / brand_metrics['Units Sold']).round(2)
        brand_metrics = brand_metrics.sort_values('Total Sales', ascending=False)

        # Display top brands
        st.markdown("**Brand Performance Summary**")
        display_brand = brand_metrics.head(25).copy()
        display_brand['Total Sales'] = display_brand['Total Sales'].apply(lambda x: f"${x:,.0f}")
        display_brand['Units Sold'] = display_brand['Units Sold'].apply(lambda x: f"{x:,.0f}")
        display_brand['Revenue/Unit'] = display_brand['Revenue/Unit'].apply(lambda x: f"${x:.2f}")
        display_brand['Avg Margin'] = display_brand['Avg Margin'].apply(lambda x: f"{x:.1f}%")
        display_brand['Avg Discount'] = display_brand['Avg Discount'].apply(lambda x: f"{x:.2f}%")

        st.dataframe(display_brand, use_container_width=True)

        st.markdown("---")

        # Brand comparison chart
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Revenue vs Margin by Brand**")
            top_brands_chart = brand_metrics.head(20).reset_index()

            fig = px.scatter(
                top_brands_chart,
                x='Units Sold',
                y='Avg Margin',
                size='Total Sales',
                color='Avg Discount',
                hover_name='Product_Brand',
                color_continuous_scale='RdYlBu_r',
                labels={'Avg Margin': 'Gross Margin (%)'}
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown("**Top Budtenders per Brand**")

            # Select a brand to see its top performers
            selected_brand_detail = st.selectbox(
                "Select brand for detailed view",
                options=brand_metrics.head(30).index.tolist(),
                key="brand_detail_select"
            )

            if selected_brand_detail:
                brand_budtenders = df[df['Product_Brand'] == selected_brand_detail].groupby('Employee').agg({
                    'Net_Sales': 'sum',
                    'Units_Sold': 'sum'
                }).sort_values('Net_Sales', ascending=False).head(10)

                fig = go.Figure(data=[go.Bar(
                    x=brand_budtenders.index,
                    y=brand_budtenders['Net_Sales'],
                    marker_color='#a29bfe',
                    text=[f"${x:,.0f}" for x in brand_budtenders['Net_Sales']],
                    textposition='auto'
                )])
                fig.update_layout(
                    height=350,
                    xaxis_tickangle=-45,
                    yaxis_title="Net Sales ($)",
                    title=f"Top 10 Budtenders for {selected_brand_detail}"
                )
                st.plotly_chart(fig, use_container_width=True)

    # ===== Performance Comparison Tab =====
    with bud_tab5:
        st.subheader("Budtender Performance Comparison")

        # Select budtenders to compare
        all_budtenders = sorted(df['Employee'].unique().tolist())

        # Default to top 5 by sales
        top_5_sales = df.groupby('Employee')['Net_Sales'].sum().sort_values(ascending=False).head(5).index.tolist()

        selected_budtenders = st.multiselect(
            "Select budtenders to compare",
            options=all_budtenders,
            default=top_5_sales[:min(5, len(top_5_sales))],
            key="budtender_compare_select"
        )

        if len(selected_budtenders) >= 2:
            compare_df = df[df['Employee'].isin(selected_budtenders)]

            # Comparison metrics
            comparison_metrics = compare_df.groupby('Employee').agg({
                'Net_Sales': 'sum',
                'Units_Sold': 'sum',
                'Product_Brand': 'nunique',
                'Gross_Margin': 'mean',
                'Discount_Pct': 'mean'
            }).round(4)
            comparison_metrics.columns = ['Total Sales', 'Units Sold', 'Brands', 'Avg Margin', 'Avg Discount']

            # Radar chart data
            st.markdown("**Performance Radar Comparison**")

            # Normalize metrics for radar chart
            normalized = comparison_metrics.copy()
            for col in normalized.columns:
                max_val = normalized[col].max()
                if max_val > 0:
                    normalized[col] = normalized[col] / max_val * 100

            fig = go.Figure()
            colors = ['#6c5ce7', '#00b894', '#e17055', '#0984e3', '#fdcb6e']

            for i, employee in enumerate(normalized.index):
                fig.add_trace(go.Scatterpolar(
                    r=normalized.loc[employee].values.tolist() + [normalized.loc[employee].values[0]],
                    theta=['Sales', 'Units', 'Brand Variety', 'Margin', 'Discount'] + ['Sales'],
                    fill='toself',
                    name=employee,
                    line_color=colors[i % len(colors)],
                    opacity=0.6
                ))

            fig.update_layout(
                polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
                showlegend=True,
                height=450
            )
            st.plotly_chart(fig, use_container_width=True)

            st.markdown("---")

            # Side-by-side metrics
            st.markdown("**Detailed Comparison**")

            comparison_display = comparison_metrics.copy()
            comparison_display['Total Sales'] = comparison_display['Total Sales'].apply(lambda x: f"${x:,.0f}")
            comparison_display['Units Sold'] = comparison_display['Units Sold'].apply(lambda x: f"{x:,.0f}")
            comparison_display['Avg Margin'] = (comparison_display['Avg Margin'] * 100).apply(lambda x: f"{x:.1f}%")
            comparison_display['Avg Discount'] = (comparison_display['Avg Discount'] * 100).apply(lambda x: f"{x:.2f}%")

            st.dataframe(comparison_display.T, use_container_width=True)

            st.markdown("---")

            # Brand overlap analysis
            st.markdown("**Brand Overlap Analysis**")

            brand_sets = {}
            for emp in selected_budtenders:
                emp_brands = set(df[df['Employee'] == emp]['Product_Brand'].unique())
                brand_sets[emp] = emp_brands

            # Calculate overlap
            if len(selected_budtenders) == 2:
                emp1, emp2 = selected_budtenders[:2]
                common_brands = brand_sets[emp1] & brand_sets[emp2]
                only_emp1 = brand_sets[emp1] - brand_sets[emp2]
                only_emp2 = brand_sets[emp2] - brand_sets[emp1]

                overlap_col1, overlap_col2, overlap_col3 = st.columns(3)
                with overlap_col1:
                    st.metric(f"Only {emp1}", len(only_emp1))
                with overlap_col2:
                    st.metric("Shared Brands", len(common_brands))
                with overlap_col3:
                    st.metric(f"Only {emp2}", len(only_emp2))

                if common_brands:
                    with st.expander("View shared brands"):
                        st.write(", ".join(sorted(common_brands)[:20]))
                        if len(common_brands) > 20:
                            st.caption(f"... and {len(common_brands) - 20} more")

        else:
            st.info("Select at least 2 budtenders to compare their performance.")

        st.markdown("---")

        # Store comparison if applicable
        if 'Store_ID' in df.columns and df['Store_ID'].nunique() > 1 and store_filter == "All Stores":
            st.markdown("**Performance by Store**")

            store_comparison = df.groupby('Store_ID').agg({
                'Employee': 'nunique',
                'Net_Sales': 'sum',
                'Units_Sold': 'sum',
                'Product_Brand': 'nunique'
            }).round(2)
            store_comparison.columns = ['Budtenders', 'Total Sales', 'Units Sold', 'Brands Sold']

            # Map store IDs to names
            store_comparison.index = store_comparison.index.map(lambda x: STORE_DISPLAY_NAMES.get(x, x))

            col1, col2 = st.columns(2)

            with col1:
                fig = go.Figure(data=[go.Bar(
                    x=store_comparison.index,
                    y=store_comparison['Total Sales'],
                    marker_color=['#6c5ce7', '#00b894'],
                    text=[f"${x:,.0f}" for x in store_comparison['Total Sales']],
                    textposition='auto'
                )])
                fig.update_layout(height=300, yaxis_title="Total Sales ($)")
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                fig = go.Figure(data=[go.Bar(
                    x=store_comparison.index,
                    y=store_comparison['Budtenders'],
                    marker_color=['#6c5ce7', '#00b894'],
                    text=store_comparison['Budtenders'],
                    textposition='auto'
                )])
                fig.update_layout(height=300, yaxis_title="Number of Budtenders")
                st.plotly_chart(fig, use_container_width=True)


def _render_customer_overview(df):
    """Render customer overview tab content."""
    st.subheader("Customer Base Overview")

    # Key metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        total_customers = len(df)
        st.metric("Total Customers", f"{total_customers:,}")

    with col2:
        if 'Lifetime Net Sales' in df.columns:
            total_ltv = df['Lifetime Net Sales'].sum()
            st.metric("Total Lifetime Value", f"${total_ltv:,.0f}")

    with col3:
        if 'Lifetime Net Sales' in df.columns:
            avg_ltv = df['Lifetime Net Sales'].mean()
            st.metric("Avg Customer LTV", f"${avg_ltv:,.0f}")

    with col4:
        if 'Lifetime Avg Order Value' in df.columns:
            avg_aov = df['Lifetime Avg Order Value'].mean()
            st.metric("Avg Order Value", f"${avg_aov:.2f}")

    st.markdown("---")

    # Customer distribution visualizations
    col1, col2 = st.columns(2)

    with col1:
        if 'Customer Segment' in df.columns:
            st.markdown("**Customer Value Distribution**")
            segment_counts = df['Customer Segment'].value_counts()
            fig = go.Figure(data=[go.Pie(
                labels=segment_counts.index,
                values=segment_counts.values,
                hole=0.4,
                marker=dict(colors=['#ff6b6b', '#4ecdc4', '#45b7d1', '#96ceb4', '#ffeaa7'])
            )])
            fig.update_layout(height=350, margin=dict(t=30, b=0, l=0, r=0))
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        if 'Recency Segment' in df.columns:
            st.markdown("**Customer Activity Status**")
            recency_counts = df['Recency Segment'].value_counts()
            fig = go.Figure(data=[go.Pie(
                labels=recency_counts.index,
                values=recency_counts.values,
                hole=0.4,
                marker=dict(colors=['#55efc4', '#74b9ff', '#a29bfe', '#fd79a8', '#636e72'])
            )])
            fig.update_layout(height=350, margin=dict(t=30, b=0, l=0, r=0))
            st.plotly_chart(fig, use_container_width=True)

    # Customer growth over time
    if 'Sign-Up Date' in df.columns:
        st.markdown("---")
        st.markdown("**Customer Acquisition Over Time**")
        df_sorted = df.sort_values('Sign-Up Date')
        df_sorted['Cumulative Customers'] = range(1, len(df_sorted) + 1)

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df_sorted['Sign-Up Date'],
            y=df_sorted['Cumulative Customers'],
            mode='lines',
            fill='tozeroy',
            line=dict(color='#6c5ce7', width=2)
        ))
        fig.update_layout(
            height=300,
            xaxis_title="Date",
            yaxis_title="Total Customers",
            margin=dict(t=30, b=0, l=0, r=0)
        )
        st.plotly_chart(fig, use_container_width=True)


def _render_customer_segments(df, analytics):
    """Render customer segments tab content."""
    st.subheader("Customer Segmentation Analysis")

    if 'Customer Segment' not in df.columns or 'Recency Segment' not in df.columns:
        st.warning("Customer segmentation data not available")
        return

    segment_order = ['Whale', 'VIP', 'Good', 'Regular', 'New/Low']

    # Segment selector
    segment_type = st.radio(
        "Segment Type",
        ["Value Segments", "Recency Segments", "Combined Matrix"],
        horizontal=True,
        key="cust_segment_type"
    )

    if segment_type == "Value Segments":
        # Value segment analysis
        segment_data = df.groupby('Customer Segment').agg({
            'Customer ID': 'count',
            'Lifetime Net Sales': ['sum', 'mean'],
            'Lifetime Transactions': 'mean',
            'Lifetime Avg Order Value': 'mean'
        }).round(2)

        segment_data.columns = ['Customer Count', 'Total Sales', 'Avg LTV', 'Avg Transactions', 'Avg Order Value']
        segment_data = segment_data.reindex([s for s in segment_order if s in segment_data.index])

        st.dataframe(segment_data, width='stretch')

        # Visualizations
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Sales Contribution by Segment**")
            fig = go.Figure(data=[go.Bar(
                x=segment_data.index,
                y=segment_data['Total Sales'],
                marker_color=['#ffeaa7', '#96ceb4', '#45b7d1', '#4ecdc4', '#ff6b6b']
            )])
            fig.update_layout(height=300, xaxis_title="Segment", yaxis_title="Total Sales ($)")
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown("**Average LTV by Segment**")
            fig = go.Figure(data=[go.Bar(
                x=segment_data.index,
                y=segment_data['Avg LTV'],
                marker_color=['#ffeaa7', '#96ceb4', '#45b7d1', '#4ecdc4', '#ff6b6b']
            )])
            fig.update_layout(height=300, xaxis_title="Segment", yaxis_title="Avg LTV ($)")
            st.plotly_chart(fig, use_container_width=True)

    elif segment_type == "Recency Segments":
        # Recency segment analysis
        recency_order = ['Active', 'Warm', 'Cool', 'Cold', 'Lost']
        recency_data = df.groupby('Recency Segment').agg({
            'Customer ID': 'count',
            'Days Since Last Visit': 'mean',
            'Lifetime Net Sales': ['sum', 'mean']
        }).round(2)

        recency_data.columns = ['Customer Count', 'Avg Days Since Visit', 'Total Sales', 'Avg LTV']
        recency_data = recency_data.reindex([s for s in recency_order if s in recency_data.index])

        st.dataframe(recency_data, width='stretch')

        # Visualizations
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Customer Count by Recency**")
            fig = go.Figure(data=[go.Bar(
                x=recency_data.index,
                y=recency_data['Customer Count'],
                marker_color=['#55efc4', '#74b9ff', '#a29bfe', '#fd79a8', '#636e72']
            )])
            fig.update_layout(height=300, xaxis_title="Recency Status", yaxis_title="Customers")
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown("**Avg Days Since Last Visit**")
            fig = go.Figure(data=[go.Bar(
                x=recency_data.index,
                y=recency_data['Avg Days Since Visit'],
                marker_color=['#55efc4', '#74b9ff', '#a29bfe', '#fd79a8', '#636e72']
            )])
            fig.update_layout(height=300, xaxis_title="Recency Status", yaxis_title="Days")
            st.plotly_chart(fig, use_container_width=True)

    else:  # Combined Matrix
        # RFM-style matrix
        st.markdown("**Customer Segment Matrix (Value × Recency)**")
        matrix = pd.crosstab(
            df['Customer Segment'],
            df['Recency Segment'],
            values=df['Customer ID'],
            aggfunc='count',
            margins=True
        )

        # Reorder for better visualization
        recency_order = ['Active', 'Warm', 'Cool', 'Cold', 'Lost']

        row_order = [s for s in segment_order if s in matrix.index] + ['All']
        col_order = [s for s in recency_order if s in matrix.columns] + ['All']

        matrix = matrix.reindex(index=row_order, columns=col_order)
        st.dataframe(matrix, width='stretch')

        # Heatmap
        matrix_no_totals = matrix.drop('All', errors='ignore').drop('All', axis=1, errors='ignore')
        fig = go.Figure(data=go.Heatmap(
            z=matrix_no_totals.values,
            x=matrix_no_totals.columns,
            y=matrix_no_totals.index,
            colorscale='Blues',
            text=matrix_no_totals.values,
            texttemplate='%{text}',
            textfont={"size": 12}
        ))
        fig.update_layout(height=400, xaxis_title="Recency", yaxis_title="Value Segment")
        st.plotly_chart(fig, use_container_width=True)

    # Demographics by Segment Analysis
    st.markdown("---")
    st.subheader("Demographics by Customer Segment")

    if 'Age' in df.columns and 'Gender' in df.columns:
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Average Age by Customer Segment**")
            age_by_segment = df.groupby('Customer Segment')['Age'].mean().reindex(segment_order)
            fig = go.Figure(data=[go.Bar(
                x=age_by_segment.index,
                y=age_by_segment.values,
                marker_color=['#ffeaa7', '#96ceb4', '#45b7d1', '#4ecdc4', '#ff6b6b'],
                text=[f"{v:.1f}" for v in age_by_segment.values],
                textposition='auto'
            )])
            fig.update_layout(height=300, xaxis_title="Segment", yaxis_title="Average Age")
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown("**Gender Distribution by Segment**")
            segment_gender = pd.crosstab(df['Customer Segment'], df['Gender'], normalize='index') * 100
            segment_gender = segment_gender.reindex([s for s in segment_order if s in segment_gender.index])

            fig = go.Figure()
            for gender in segment_gender.columns:
                fig.add_trace(go.Bar(
                    name=gender,
                    x=segment_gender.index,
                    y=segment_gender[gender],
                    text=[f"{v:.1f}%" for v in segment_gender[gender]],
                    textposition='auto'
                ))

            fig.update_layout(
                height=300,
                barmode='stack',
                xaxis_title="Segment",
                yaxis_title="Percentage (%)",
                showlegend=True
            )
            st.plotly_chart(fig, use_container_width=True)

    if 'Age' in df.columns:
        st.markdown("---")
        st.markdown("**Age Distribution Across Segments**")

        fig = go.Figure()
        for segment in [s for s in segment_order if s in df['Customer Segment'].unique()]:
            segment_data = df[df['Customer Segment'] == segment]['Age']
            fig.add_trace(go.Box(
                y=segment_data,
                name=segment,
                boxmean='sd'
            ))

        fig.update_layout(height=350, yaxis_title="Age", xaxis_title="Customer Segment")
        st.plotly_chart(fig, use_container_width=True)

    if 'City' in df.columns:
        st.markdown("---")
        st.markdown("**Top Cities by Customer Segment**")

        top_cities = df['City'].value_counts().head(5).index.tolist()

        city_segment_data = []
        for city in top_cities:
            for segment in [s for s in segment_order if s in df['Customer Segment'].unique()]:
                count = len(df[(df['City'] == city) & (df['Customer Segment'] == segment)])
                city_segment_data.append({
                    'City': city,
                    'Segment': segment,
                    'Count': count
                })

        city_df = pd.DataFrame(city_segment_data)

        fig = go.Figure()
        for segment in [s for s in segment_order if s in df['Customer Segment'].unique()]:
            seg_data = city_df[city_df['Segment'] == segment]
            fig.add_trace(go.Bar(
                name=segment,
                x=seg_data['City'],
                y=seg_data['Count']
            ))

        fig.update_layout(
            height=350,
            barmode='group',
            xaxis_title="City",
            yaxis_title="Customer Count",
            showlegend=True
        )
        st.plotly_chart(fig, use_container_width=True)


def _render_customer_demographics(df):
    """Render customer demographics tab content."""
    st.subheader("Customer Demographics")

    col1, col2, col3 = st.columns(3)

    # Age distribution
    with col1:
        if 'Age' in df.columns:
            st.markdown("**Age Distribution**")
            avg_age = df['Age'].mean()
            median_age = df['Age'].median()

            st.metric("Average Age", f"{avg_age:.1f} years")
            st.metric("Median Age", f"{median_age:.0f} years")

            fig = go.Figure(data=[go.Histogram(
                x=df['Age'],
                nbinsx=20,
                marker_color='#6c5ce7'
            )])
            fig.update_layout(height=250, xaxis_title="Age", yaxis_title="Count", margin=dict(t=20, b=0))
            st.plotly_chart(fig, use_container_width=True)

    # Gender distribution
    with col2:
        if 'Gender' in df.columns:
            st.markdown("**Gender Distribution**")
            gender_counts = df['Gender'].value_counts()

            for gender, count in gender_counts.items():
                pct = (count / len(df)) * 100
                st.metric(gender, f"{count} ({pct:.1f}%)")

            fig = go.Figure(data=[go.Pie(
                labels=gender_counts.index,
                values=gender_counts.values,
                hole=0.4
            )])
            fig.update_layout(height=250, margin=dict(t=20, b=0, l=0, r=0))
            st.plotly_chart(fig, use_container_width=True)

    # Customer type
    with col3:
        if 'Customer Status' in df.columns:
            st.markdown("**Customer Type**")
            type_counts = df['Customer Status'].value_counts()

            for ctype, count in type_counts.items():
                pct = (count / len(df)) * 100
                st.metric(str(ctype)[:15], f"{count} ({pct:.1f}%)")

            fig = go.Figure(data=[go.Pie(
                labels=type_counts.index,
                values=type_counts.values,
                hole=0.4
            )])
            fig.update_layout(height=250, margin=dict(t=20, b=0, l=0, r=0))
            st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # Geographic distribution
    if 'City' in df.columns:
        st.markdown("**Geographic Distribution**")
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Top Cities**")
            top_cities = df['City'].value_counts().head(10)
            st.dataframe(top_cities, width='stretch')

        with col2:
            if 'State' in df.columns:
                st.markdown("**States/Regions**")
                state_counts = df['State'].value_counts().head(10)
                fig = go.Figure(data=[go.Bar(
                    x=state_counts.index,
                    y=state_counts.values,
                    marker_color='#00b894'
                )])
                fig.update_layout(height=300, xaxis_title="State", yaxis_title="Customers")
                st.plotly_chart(fig, use_container_width=True)


def _render_customer_ltv(df):
    """Render customer lifetime value tab content."""
    st.subheader("Customer Lifetime Value Analysis")

    if 'Lifetime Net Sales' not in df.columns:
        st.warning("Lifetime value data not available")
        return

    # LTV metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        total_ltv = df['Lifetime Net Sales'].sum()
        st.metric("Total LTV", f"${total_ltv:,.0f}")

    with col2:
        avg_ltv = df['Lifetime Net Sales'].mean()
        st.metric("Average LTV", f"${avg_ltv:,.0f}")

    with col3:
        median_ltv = df['Lifetime Net Sales'].median()
        st.metric("Median LTV", f"${median_ltv:,.0f}")

    with col4:
        top_10_pct_ltv = df.nlargest(int(len(df) * 0.1), 'Lifetime Net Sales')['Lifetime Net Sales'].sum()
        top_10_pct = (top_10_pct_ltv / total_ltv) * 100
        st.metric("Top 10% Contribution", f"{top_10_pct:.1f}%")

    st.markdown("---")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**LTV Distribution**")
        fig = go.Figure(data=[go.Histogram(
            x=df['Lifetime Net Sales'],
            nbinsx=50,
            marker_color='#fdcb6e'
        )])
        fig.update_layout(height=300, xaxis_title="Lifetime Value ($)", yaxis_title="Customers")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("**Top 20 Customers by LTV**")
        top_customers = df.nlargest(20, 'Lifetime Net Sales')[
            ['Customer Name', 'Lifetime Net Sales', 'Customer Segment']
        ] if 'Customer Name' in df.columns else df.nlargest(20, 'Lifetime Net Sales')[
            ['Lifetime Net Sales', 'Customer Segment']
        ]
        st.dataframe(top_customers, width='stretch', height=300)

    # LTV by segment
    if 'Customer Segment' in df.columns:
        st.markdown("---")
        st.markdown("**LTV Distribution by Customer Segment**")

        fig = go.Figure()
        for segment in df['Customer Segment'].unique():
            segment_data = df[df['Customer Segment'] == segment]['Lifetime Net Sales']
            fig.add_trace(go.Box(
                y=segment_data,
                name=segment,
                boxmean='sd'
            ))

        fig.update_layout(height=400, yaxis_title="Lifetime Value ($)", xaxis_title="Segment")
        st.plotly_chart(fig, use_container_width=True)


def _render_customer_recency(df):
    """Render customer recency & retention tab content."""
    st.subheader("Customer Recency & Retention")

    if 'Days Since Last Visit' not in df.columns:
        st.warning("Recency data not available")
        return

    # Recency metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        active_customers = len(df[df['Days Since Last Visit'] <= 30]) if 'Days Since Last Visit' in df.columns else 0
        st.metric("Active (30d)", active_customers)

    with col2:
        at_risk = len(df[df['Days Since Last Visit'] > 90]) if 'Days Since Last Visit' in df.columns else 0
        st.metric("At Risk (90d+)", at_risk)

    with col3:
        avg_days_since = df['Days Since Last Visit'].mean()
        st.metric("Avg Days Since Visit", f"{avg_days_since:.0f}")

    with col4:
        if 'Lifetime In-Store Visits' in df.columns:
            avg_visits = df['Lifetime In-Store Visits'].mean()
            st.metric("Avg Lifetime Visits", f"{avg_visits:.1f}")

    st.markdown("---")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Days Since Last Visit Distribution**")
        fig = go.Figure(data=[go.Histogram(
            x=df['Days Since Last Visit'],
            nbinsx=30,
            marker_color='#e17055'
        )])
        fig.update_layout(height=300, xaxis_title="Days Since Last Visit", yaxis_title="Customers")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        if 'Lifetime In-Store Visits' in df.columns:
            st.markdown("**Visit Frequency Distribution**")
            fig = go.Figure(data=[go.Histogram(
                x=df['Lifetime In-Store Visits'],
                nbinsx=30,
                marker_color='#00b894'
            )])
            fig.update_layout(height=300, xaxis_title="Lifetime Visits", yaxis_title="Customers")
            st.plotly_chart(fig, use_container_width=True)

    # Churn risk analysis
    st.markdown("---")
    st.markdown("**Churn Risk Analysis**")

    if 'Recency Segment' in df.columns and 'Customer Segment' in df.columns:
        # At-risk high value customers
        at_risk_vip = df[
            (df['Recency Segment'].isin(['Cold', 'Lost'])) &
            (df['Customer Segment'].isin(['VIP', 'Whale']))
        ]

        if len(at_risk_vip) > 0:
            st.warning(f"⚠️ {len(at_risk_vip)} high-value customers are at risk of churning!")

            cols = ['Customer Name', 'Customer Segment', 'Recency Segment',
                   'Lifetime Net Sales', 'Days Since Last Visit']
            display_cols = [c for c in cols if c in at_risk_vip.columns]

            st.dataframe(
                at_risk_vip[display_cols].head(20),
                width='stretch'
            )


def _render_customer_search(df):
    """Render customer search tab content."""
    st.subheader("Customer Search & Filter")

    # Search and filter options
    col1, col2, col3 = st.columns(3)

    with col1:
        if 'Customer Name' in df.columns:
            search_name = st.text_input("Search by Name", "", key="cust_search_name")

    with col2:
        if 'Customer Segment' in df.columns:
            segment_filter = st.multiselect(
                "Filter by Segment",
                options=df['Customer Segment'].unique().tolist(),
                default=[],
                key="cust_segment_filter"
            )

    with col3:
        if 'Recency Segment' in df.columns:
            recency_filter = st.multiselect(
                "Filter by Recency",
                options=df['Recency Segment'].unique().tolist(),
                default=[],
                key="cust_recency_filter"
            )

    # Apply filters
    filtered_df = df.copy()

    if 'Customer Name' in df.columns and 'search_name' in dir() and search_name:
        filtered_df = filtered_df[
            filtered_df['Customer Name'].str.contains(search_name, case=False, na=False)
        ]

    if 'segment_filter' in dir() and segment_filter and 'Customer Segment' in df.columns:
        filtered_df = filtered_df[filtered_df['Customer Segment'].isin(segment_filter)]

    if 'recency_filter' in dir() and recency_filter and 'Recency Segment' in df.columns:
        filtered_df = filtered_df[filtered_df['Recency Segment'].isin(recency_filter)]

    st.info(f"Showing {len(filtered_df)} of {len(df)} customers")

    # Display filtered results
    display_cols = [
        'Customer Name', 'Email', 'Phone', 'Customer Segment', 'Recency Segment',
        'Lifetime Net Sales', 'Lifetime Transactions', 'Days Since Last Visit',
        'Age', 'Gender', 'City', 'State'
    ]
    display_cols = [c for c in display_cols if c in filtered_df.columns]

    st.dataframe(
        filtered_df[display_cols],
        width='stretch',
        height=400
    )

    # Download filtered data
    csv = filtered_df.to_csv(index=False)
    st.download_button(
        "Download Filtered Customer Data",
        csv,
        "filtered_customers.csv",
        "text/csv",
        key="cust_download_btn"
    )


def _render_customer_analytics_content(state, analytics, store_filter, date_filter=None):
    """Render customer analytics content (used as nested tab within Sales Analytics)."""
    if state.customer_data is None:
        st.warning("Please upload customer data first using the 'Data Center' page.")
        st.info("Upload a CSV file containing customer demographics, transaction history, and loyalty information.")
        return

    df = state.customer_data.copy()

    # Apply store filter
    if store_filter != "All Stores":
        store_id = 'barbary_coast' if store_filter == "Barbary Coast" else 'grass_roots'
        if 'Store_ID' in df.columns:
            df = df[df['Store_ID'] == store_id]

    st.info(f"Analyzing {len(df)} customers")

    # Create subtabs for different analytics views
    cust_tab1, cust_tab2, cust_tab3, cust_tab4, cust_tab5, cust_tab6 = st.tabs([
        "Overview",
        "Customer Segments",
        "Demographics",
        "Lifetime Value",
        "Recency & Retention",
        "Customer Search"
    ])

    # ===== Overview =====
    with cust_tab1:
        _render_customer_overview(df)

    # ===== Customer Segments =====
    with cust_tab2:
        _render_customer_segments(df, analytics)

    # ===== Demographics =====
    with cust_tab3:
        _render_customer_demographics(df)

    # ===== Lifetime Value =====
    with cust_tab4:
        _render_customer_ltv(df)

    # ===== Recency & Retention =====
    with cust_tab5:
        _render_customer_recency(df)

    # ===== Customer Search =====
    with cust_tab6:
        _render_customer_search(df)


def render_customer_analytics(state, analytics, store_filter, date_filter=None):
    """Render comprehensive customer analytics dashboard."""
    st.header("Customer Analytics")

    if state.customer_data is None:
        st.warning("Please upload customer data first using the 'Data Center' page.")
        st.info("💡 Upload a CSV file containing customer demographics, transaction history, and loyalty information.")
        return

    df = state.customer_data.copy()

    # Apply store filter
    if store_filter != "All Stores":
        store_id = 'barbary_coast' if store_filter == "Barbary Coast" else 'grass_roots'
        if 'Store_ID' in df.columns:
            df = df[df['Store_ID'] == store_id]

    st.info(f"📊 Analyzing {len(df)} customers")

    # Create tabs for different analytics views
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📊 Overview",
        "💎 Customer Segments",
        "📍 Demographics",
        "💰 Lifetime Value",
        "🔄 Recency & Retention",
        "🔍 Customer Search"
    ])

    # ===== TAB 1: Overview =====
    with tab1:
        st.subheader("Customer Base Overview")

        # Key metrics
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            total_customers = len(df)
            st.metric("Total Customers", f"{total_customers:,}")

        with col2:
            if 'Lifetime Net Sales' in df.columns:
                total_ltv = df['Lifetime Net Sales'].sum()
                st.metric("Total Lifetime Value", f"${total_ltv:,.0f}")

        with col3:
            if 'Lifetime Net Sales' in df.columns:
                avg_ltv = df['Lifetime Net Sales'].mean()
                st.metric("Avg Customer LTV", f"${avg_ltv:,.0f}")

        with col4:
            if 'Lifetime Avg Order Value' in df.columns:
                avg_aov = df['Lifetime Avg Order Value'].mean()
                st.metric("Avg Order Value", f"${avg_aov:.2f}")

        st.markdown("---")

        # Customer distribution visualizations
        col1, col2 = st.columns(2)

        with col1:
            if 'Customer Segment' in df.columns:
                st.markdown("**Customer Value Distribution**")
                segment_counts = df['Customer Segment'].value_counts()
                fig = go.Figure(data=[go.Pie(
                    labels=segment_counts.index,
                    values=segment_counts.values,
                    hole=0.4,
                    marker=dict(colors=['#ff6b6b', '#4ecdc4', '#45b7d1', '#96ceb4', '#ffeaa7'])
                )])
                fig.update_layout(height=350, margin=dict(t=30, b=0, l=0, r=0))
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            if 'Recency Segment' in df.columns:
                st.markdown("**Customer Activity Status**")
                recency_counts = df['Recency Segment'].value_counts()
                fig = go.Figure(data=[go.Pie(
                    labels=recency_counts.index,
                    values=recency_counts.values,
                    hole=0.4,
                    marker=dict(colors=['#55efc4', '#74b9ff', '#a29bfe', '#fd79a8', '#636e72'])
                )])
                fig.update_layout(height=350, margin=dict(t=30, b=0, l=0, r=0))
                st.plotly_chart(fig, use_container_width=True)

        # Customer growth over time
        if 'Sign-Up Date' in df.columns:
            st.markdown("---")
            st.markdown("**Customer Acquisition Over Time**")
            df_sorted = df.sort_values('Sign-Up Date')
            df_sorted['Cumulative Customers'] = range(1, len(df_sorted) + 1)

            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=df_sorted['Sign-Up Date'],
                y=df_sorted['Cumulative Customers'],
                mode='lines',
                fill='tozeroy',
                line=dict(color='#6c5ce7', width=2)
            ))
            fig.update_layout(
                height=300,
                xaxis_title="Date",
                yaxis_title="Total Customers",
                margin=dict(t=30, b=0, l=0, r=0)
            )
            st.plotly_chart(fig, use_container_width=True)

    # ===== TAB 2: Customer Segments =====
    with tab2:
        st.subheader("Customer Segmentation Analysis")

        if 'Customer Segment' not in df.columns or 'Recency Segment' not in df.columns:
            st.warning("Customer segmentation data not available")
            return

        # Segment selector
        segment_type = st.radio(
            "Segment Type",
            ["Value Segments", "Recency Segments", "Combined Matrix"],
            horizontal=True
        )

        if segment_type == "Value Segments":
            # Value segment analysis
            segment_order = ['Whale', 'VIP', 'Good', 'Regular', 'New/Low']
            segment_data = df.groupby('Customer Segment').agg({
                'Customer ID': 'count',
                'Lifetime Net Sales': ['sum', 'mean'],
                'Lifetime Transactions': 'mean',
                'Lifetime Avg Order Value': 'mean'
            }).round(2)

            segment_data.columns = ['Customer Count', 'Total Sales', 'Avg LTV', 'Avg Transactions', 'Avg Order Value']
            segment_data = segment_data.reindex([s for s in segment_order if s in segment_data.index])

            st.dataframe(segment_data, width='stretch')

            # Visualizations
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("**Sales Contribution by Segment**")
                fig = go.Figure(data=[go.Bar(
                    x=segment_data.index,
                    y=segment_data['Total Sales'],
                    marker_color=['#ffeaa7', '#96ceb4', '#45b7d1', '#4ecdc4', '#ff6b6b']
                )])
                fig.update_layout(height=300, xaxis_title="Segment", yaxis_title="Total Sales ($)")
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                st.markdown("**Average LTV by Segment**")
                fig = go.Figure(data=[go.Bar(
                    x=segment_data.index,
                    y=segment_data['Avg LTV'],
                    marker_color=['#ffeaa7', '#96ceb4', '#45b7d1', '#4ecdc4', '#ff6b6b']
                )])
                fig.update_layout(height=300, xaxis_title="Segment", yaxis_title="Avg LTV ($)")
                st.plotly_chart(fig, use_container_width=True)

        elif segment_type == "Recency Segments":
            # Recency segment analysis
            recency_order = ['Active', 'Warm', 'Cool', 'Cold', 'Lost']
            recency_data = df.groupby('Recency Segment').agg({
                'Customer ID': 'count',
                'Days Since Last Visit': 'mean',
                'Lifetime Net Sales': ['sum', 'mean']
            }).round(2)

            recency_data.columns = ['Customer Count', 'Avg Days Since Visit', 'Total Sales', 'Avg LTV']
            recency_data = recency_data.reindex([s for s in recency_order if s in recency_data.index])

            st.dataframe(recency_data, width='stretch')

            # Visualizations
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("**Customer Count by Recency**")
                fig = go.Figure(data=[go.Bar(
                    x=recency_data.index,
                    y=recency_data['Customer Count'],
                    marker_color=['#55efc4', '#74b9ff', '#a29bfe', '#fd79a8', '#636e72']
                )])
                fig.update_layout(height=300, xaxis_title="Recency Status", yaxis_title="Customers")
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                st.markdown("**Avg Days Since Last Visit**")
                fig = go.Figure(data=[go.Bar(
                    x=recency_data.index,
                    y=recency_data['Avg Days Since Visit'],
                    marker_color=['#55efc4', '#74b9ff', '#a29bfe', '#fd79a8', '#636e72']
                )])
                fig.update_layout(height=300, xaxis_title="Recency Status", yaxis_title="Days")
                st.plotly_chart(fig, use_container_width=True)

        else:  # Combined Matrix
            # RFM-style matrix
            st.markdown("**Customer Segment Matrix (Value × Recency)**")
            matrix = pd.crosstab(
                df['Customer Segment'],
                df['Recency Segment'],
                values=df['Customer ID'],
                aggfunc='count',
                margins=True
            )

            # Reorder for better visualization
            segment_order = ['Whale', 'VIP', 'Good', 'Regular', 'New/Low']
            recency_order = ['Active', 'Warm', 'Cool', 'Cold', 'Lost']

            row_order = [s for s in segment_order if s in matrix.index] + ['All']
            col_order = [s for s in recency_order if s in matrix.columns] + ['All']

            matrix = matrix.reindex(index=row_order, columns=col_order)
            st.dataframe(matrix, width='stretch')

            # Heatmap
            matrix_no_totals = matrix.drop('All', errors='ignore').drop('All', axis=1, errors='ignore')
            fig = go.Figure(data=go.Heatmap(
                z=matrix_no_totals.values,
                x=matrix_no_totals.columns,
                y=matrix_no_totals.index,
                colorscale='Blues',
                text=matrix_no_totals.values,
                texttemplate='%{text}',
                textfont={"size": 12}
            ))
            fig.update_layout(height=400, xaxis_title="Recency", yaxis_title="Value Segment")
            st.plotly_chart(fig, use_container_width=True)

        # Demographics by Segment Analysis
        st.markdown("---")
        st.subheader("📊 Demographics by Customer Segment")

        if 'Age' in df.columns and 'Gender' in df.columns:
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("**Average Age by Customer Segment**")
                age_by_segment = df.groupby('Customer Segment')['Age'].mean().reindex(segment_order)
                fig = go.Figure(data=[go.Bar(
                    x=age_by_segment.index,
                    y=age_by_segment.values,
                    marker_color=['#ffeaa7', '#96ceb4', '#45b7d1', '#4ecdc4', '#ff6b6b'],
                    text=[f"{v:.1f}" for v in age_by_segment.values],
                    textposition='auto'
                )])
                fig.update_layout(height=300, xaxis_title="Segment", yaxis_title="Average Age")
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                st.markdown("**Gender Distribution by Segment**")
                # Calculate gender percentages for each segment
                segment_gender = pd.crosstab(df['Customer Segment'], df['Gender'], normalize='index') * 100
                segment_gender = segment_gender.reindex([s for s in segment_order if s in segment_gender.index])

                fig = go.Figure()
                for gender in segment_gender.columns:
                    fig.add_trace(go.Bar(
                        name=gender,
                        x=segment_gender.index,
                        y=segment_gender[gender],
                        text=[f"{v:.1f}%" for v in segment_gender[gender]],
                        textposition='auto'
                    ))

                fig.update_layout(
                    height=300,
                    barmode='stack',
                    xaxis_title="Segment",
                    yaxis_title="Percentage (%)",
                    showlegend=True
                )
                st.plotly_chart(fig, use_container_width=True)

        # More demographic comparisons
        if 'Age' in df.columns:
            st.markdown("---")
            st.markdown("**Age Distribution Across Segments**")

            fig = go.Figure()
            for segment in [s for s in segment_order if s in df['Customer Segment'].unique()]:
                segment_data = df[df['Customer Segment'] == segment]['Age']
                fig.add_trace(go.Box(
                    y=segment_data,
                    name=segment,
                    boxmean='sd'
                ))

            fig.update_layout(height=350, yaxis_title="Age", xaxis_title="Customer Segment")
            st.plotly_chart(fig, use_container_width=True)

        # Geographic distribution by segment
        if 'City' in df.columns:
            st.markdown("---")
            st.markdown("**Top Cities by Customer Segment**")

            # Get top 5 cities overall
            top_cities = df['City'].value_counts().head(5).index.tolist()

            # Count customers by segment and city
            city_segment_data = []
            for city in top_cities:
                for segment in [s for s in segment_order if s in df['Customer Segment'].unique()]:
                    count = len(df[(df['City'] == city) & (df['Customer Segment'] == segment)])
                    city_segment_data.append({
                        'City': city,
                        'Segment': segment,
                        'Count': count
                    })

            city_df = pd.DataFrame(city_segment_data)

            fig = go.Figure()
            for segment in [s for s in segment_order if s in df['Customer Segment'].unique()]:
                segment_data = city_df[city_df['Segment'] == segment]
                fig.add_trace(go.Bar(
                    name=segment,
                    x=segment_data['City'],
                    y=segment_data['Count']
                ))

            fig.update_layout(
                height=350,
                barmode='group',
                xaxis_title="City",
                yaxis_title="Customer Count",
                showlegend=True
            )
            st.plotly_chart(fig, use_container_width=True)

    # ===== TAB 3: Demographics =====
    with tab3:
        st.subheader("Customer Demographics")

        col1, col2, col3 = st.columns(3)

        # Age distribution
        with col1:
            if 'Age' in df.columns:
                st.markdown("**Age Distribution**")
                avg_age = df['Age'].mean()
                median_age = df['Age'].median()

                st.metric("Average Age", f"{avg_age:.1f} years")
                st.metric("Median Age", f"{median_age:.0f} years")

                fig = go.Figure(data=[go.Histogram(
                    x=df['Age'],
                    nbinsx=20,
                    marker_color='#6c5ce7'
                )])
                fig.update_layout(height=250, xaxis_title="Age", yaxis_title="Count", margin=dict(t=20, b=0))
                st.plotly_chart(fig, use_container_width=True)

        # Gender distribution
        with col2:
            if 'Gender' in df.columns:
                st.markdown("**Gender Distribution**")
                gender_counts = df['Gender'].value_counts()

                for gender, count in gender_counts.items():
                    pct = (count / len(df)) * 100
                    st.metric(gender, f"{count} ({pct:.1f}%)")

                fig = go.Figure(data=[go.Pie(
                    labels=gender_counts.index,
                    values=gender_counts.values,
                    hole=0.4
                )])
                fig.update_layout(height=250, margin=dict(t=20, b=0, l=0, r=0))
                st.plotly_chart(fig, use_container_width=True)

        # Customer type
        with col3:
            if 'Customer Status' in df.columns:
                st.markdown("**Customer Type**")
                type_counts = df['Customer Status'].value_counts()

                for ctype, count in type_counts.items():
                    pct = (count / len(df)) * 100
                    st.metric(str(ctype)[:15], f"{count} ({pct:.1f}%)")

                fig = go.Figure(data=[go.Pie(
                    labels=type_counts.index,
                    values=type_counts.values,
                    hole=0.4
                )])
                fig.update_layout(height=250, margin=dict(t=20, b=0, l=0, r=0))
                st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")

        # Geographic distribution
        if 'City' in df.columns:
            st.markdown("**Geographic Distribution**")
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("**Top Cities**")
                top_cities = df['City'].value_counts().head(10)
                st.dataframe(top_cities, width='stretch')

            with col2:
                if 'State' in df.columns:
                    st.markdown("**States/Regions**")
                    state_counts = df['State'].value_counts().head(10)
                    fig = go.Figure(data=[go.Bar(
                        x=state_counts.index,
                        y=state_counts.values,
                        marker_color='#00b894'
                    )])
                    fig.update_layout(height=300, xaxis_title="State", yaxis_title="Customers")
                    st.plotly_chart(fig, use_container_width=True)

    # ===== TAB 4: Lifetime Value =====
    with tab4:
        st.subheader("Customer Lifetime Value Analysis")

        if 'Lifetime Net Sales' not in df.columns:
            st.warning("Lifetime value data not available")
            return

        # LTV metrics
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            total_ltv = df['Lifetime Net Sales'].sum()
            st.metric("Total LTV", f"${total_ltv:,.0f}")

        with col2:
            avg_ltv = df['Lifetime Net Sales'].mean()
            st.metric("Average LTV", f"${avg_ltv:,.0f}")

        with col3:
            median_ltv = df['Lifetime Net Sales'].median()
            st.metric("Median LTV", f"${median_ltv:,.0f}")

        with col4:
            top_10_pct_ltv = df.nlargest(int(len(df) * 0.1), 'Lifetime Net Sales')['Lifetime Net Sales'].sum()
            top_10_pct = (top_10_pct_ltv / total_ltv) * 100
            st.metric("Top 10% Contribution", f"{top_10_pct:.1f}%")

        st.markdown("---")

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**LTV Distribution**")
            fig = go.Figure(data=[go.Histogram(
                x=df['Lifetime Net Sales'],
                nbinsx=50,
                marker_color='#fdcb6e'
            )])
            fig.update_layout(height=300, xaxis_title="Lifetime Value ($)", yaxis_title="Customers")
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown("**Top 20 Customers by LTV**")
            top_customers = df.nlargest(20, 'Lifetime Net Sales')[
                ['Customer Name', 'Lifetime Net Sales', 'Customer Segment']
            ] if 'Customer Name' in df.columns else df.nlargest(20, 'Lifetime Net Sales')[
                ['Lifetime Net Sales', 'Customer Segment']
            ]
            st.dataframe(top_customers, width='stretch', height=300)

        # LTV by segment
        if 'Customer Segment' in df.columns:
            st.markdown("---")
            st.markdown("**LTV Distribution by Customer Segment**")

            fig = go.Figure()
            for segment in df['Customer Segment'].unique():
                segment_data = df[df['Customer Segment'] == segment]['Lifetime Net Sales']
                fig.add_trace(go.Box(
                    y=segment_data,
                    name=segment,
                    boxmean='sd'
                ))

            fig.update_layout(height=400, yaxis_title="Lifetime Value ($)", xaxis_title="Segment")
            st.plotly_chart(fig, use_container_width=True)

    # ===== TAB 5: Recency & Retention =====
    with tab5:
        st.subheader("Customer Recency & Retention")

        if 'Days Since Last Visit' not in df.columns:
            st.warning("Recency data not available")
            return

        # Recency metrics
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            active_customers = len(df[df['Days Since Last Visit'] <= 30]) if 'Days Since Last Visit' in df.columns else 0
            st.metric("Active (30d)", active_customers)

        with col2:
            at_risk = len(df[df['Days Since Last Visit'] > 90]) if 'Days Since Last Visit' in df.columns else 0
            st.metric("At Risk (90d+)", at_risk)

        with col3:
            avg_days_since = df['Days Since Last Visit'].mean()
            st.metric("Avg Days Since Visit", f"{avg_days_since:.0f}")

        with col4:
            if 'Lifetime In-Store Visits' in df.columns:
                avg_visits = df['Lifetime In-Store Visits'].mean()
                st.metric("Avg Lifetime Visits", f"{avg_visits:.1f}")

        st.markdown("---")

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Days Since Last Visit Distribution**")
            fig = go.Figure(data=[go.Histogram(
                x=df['Days Since Last Visit'],
                nbinsx=30,
                marker_color='#e17055'
            )])
            fig.update_layout(height=300, xaxis_title="Days Since Last Visit", yaxis_title="Customers")
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            if 'Lifetime In-Store Visits' in df.columns:
                st.markdown("**Visit Frequency Distribution**")
                fig = go.Figure(data=[go.Histogram(
                    x=df['Lifetime In-Store Visits'],
                    nbinsx=30,
                    marker_color='#00b894'
                )])
                fig.update_layout(height=300, xaxis_title="Lifetime Visits", yaxis_title="Customers")
                st.plotly_chart(fig, use_container_width=True)

        # Churn risk analysis
        st.markdown("---")
        st.markdown("**Churn Risk Analysis**")

        if 'Recency Segment' in df.columns and 'Customer Segment' in df.columns:
            # At-risk high value customers
            at_risk_vip = df[
                (df['Recency Segment'].isin(['Cold', 'Lost'])) &
                (df['Customer Segment'].isin(['VIP', 'Whale']))
            ]

            if len(at_risk_vip) > 0:
                st.warning(f"⚠️ {len(at_risk_vip)} high-value customers are at risk of churning!")

                cols = ['Customer Name', 'Customer Segment', 'Recency Segment',
                       'Lifetime Net Sales', 'Days Since Last Visit']
                display_cols = [c for c in cols if c in at_risk_vip.columns]

                st.dataframe(
                    at_risk_vip[display_cols].head(20),
                    width='stretch'
                )

    # ===== TAB 6: Customer Search =====
    with tab6:
        st.subheader("Customer Search & Filter")

        # Search and filter options
        col1, col2, col3 = st.columns(3)

        with col1:
            if 'Customer Name' in df.columns:
                search_name = st.text_input("Search by Name", "")

        with col2:
            if 'Customer Segment' in df.columns:
                segment_filter = st.multiselect(
                    "Filter by Segment",
                    options=df['Customer Segment'].unique().tolist(),
                    default=[]
                )

        with col3:
            if 'Recency Segment' in df.columns:
                recency_filter = st.multiselect(
                    "Filter by Recency",
                    options=df['Recency Segment'].unique().tolist(),
                    default=[]
                )

        # Apply filters
        filtered_df = df.copy()

        if 'Customer Name' in df.columns and search_name:
            filtered_df = filtered_df[
                filtered_df['Customer Name'].str.contains(search_name, case=False, na=False)
            ]

        if segment_filter and 'Customer Segment' in df.columns:
            filtered_df = filtered_df[filtered_df['Customer Segment'].isin(segment_filter)]

        if recency_filter and 'Recency Segment' in df.columns:
            filtered_df = filtered_df[filtered_df['Recency Segment'].isin(recency_filter)]

        st.info(f"Showing {len(filtered_df)} of {len(df)} customers")

        # Display filtered results
        display_cols = [
            'Customer Name', 'Email', 'Phone', 'Customer Segment', 'Recency Segment',
            'Lifetime Net Sales', 'Lifetime Transactions', 'Days Since Last Visit',
            'Age', 'Gender', 'City', 'State'
        ]
        display_cols = [c for c in display_cols if c in filtered_df.columns]

        st.dataframe(
            filtered_df[display_cols],
            width='stretch',
            height=400
        )

        # Download filtered data
        csv = filtered_df.to_csv(index=False)
        st.download_button(
            "📥 Download Filtered Customer Data",
            csv,
            "filtered_customers.csv",
            "text/csv"
        )


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
        st.dataframe(top_brands, width='stretch')
    
    with col2:
        st.subheader("⚠️ Low Margin Brands")
        underperformers = analytics.identify_underperformers(df)
        st.dataframe(underperformers, width='stretch')
    
    # Margin vs Sales scatter
    st.subheader("Margin vs. Sales Analysis")

    # Filter to significant brands with valid margin data
    significant_brands = df[
        (df['Net Sales'] > 1000) &  # Lowered threshold
        (df['Gross Margin %'].notna()) &
        (df['Gross Margin %'] > 0)
    ].copy()

    # Handle margin percentage - check if already in percentage form or decimal
    if len(significant_brands) > 0:
        max_margin = significant_brands['Gross Margin %'].max()
        if max_margin <= 1:
            # Decimal form (0.55), convert to percentage
            significant_brands['Margin_Pct'] = significant_brands['Gross Margin %'] * 100
        else:
            # Already percentage form (55)
            significant_brands['Margin_Pct'] = significant_brands['Gross Margin %']

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
    else:
        st.info("No brand data with sufficient sales volume to display.")
    
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
        st.dataframe(df, width='stretch')
    
    # Category bar chart
    fig = px.bar(df, x='Product Type', y='Net Sales',
                title='Net Sales by Product Category',
                color='Net Sales',
                color_continuous_scale='Blues')
    st.plotly_chart(fig, use_container_width=True)


@st.cache_data(ttl=86400, show_spinner=False)  # Cache for 24 hours
def _fetch_invoice_data_cached(_aws_access_key: str, _aws_secret_key: str, _region: str):
    """Fetch invoice data with daily caching to avoid repeated DynamoDB scans."""
    try:
        invoice_service = InvoiceDataService(
            aws_access_key=_aws_access_key,
            aws_secret_key=_aws_secret_key,
            region=_region
        )
        invoice_summary = invoice_service.get_invoice_summary()
        product_summary = invoice_service.get_product_summary()
        # Return with timestamp for display
        return invoice_summary, product_summary, datetime.now().strftime("%Y-%m-%d %H:%M")
    except Exception as e:
        return None, None, None


@st.cache_data(ttl=86400, show_spinner=False)  # Cache for 24 hours
def _fetch_research_data_cached(_aws_access_key: str, _aws_secret_key: str, _region: str):
    """Fetch research findings with daily caching."""
    try:
        research_viewer = ResearchFindingsViewer()
        if research_viewer.is_available():
            return research_viewer.load_latest_summary()
        return None
    except Exception:
        return None


@st.cache_data(ttl=86400, show_spinner=False)  # Cache for 24 hours
def _fetch_seo_data_cached(_aws_access_key: str, _aws_secret_key: str, _region: str):
    """Fetch SEO data for both sites with daily caching."""
    try:
        seo_data = {}
        for site_name, site_url in [("Barbary Coast", "https://barbarycoastsf.com"),
                                    ("Grass Roots", "https://grassrootssf.com")]:
            seo_viewer = SEOFindingsViewer(website=site_url)
            if seo_viewer.is_available():
                seo_summary = seo_viewer.load_latest_summary()
                if seo_summary:
                    seo_data[site_name] = {
                        'overall_score': seo_summary.get('overall_score', 0),
                        'categories': seo_summary.get('categories', {}),
                        'top_priorities': seo_summary.get('top_priorities', []),
                        'quick_wins': seo_summary.get('quick_wins', [])
                    }
        return seo_data if seo_data else None
    except Exception:
        return None


@st.cache_data(ttl=86400, show_spinner=False)  # Cache for 24 hours
def _fetch_research_documents_cached(_aws_access_key: str, _aws_secret_key: str, _region: str):
    """Fetch list of research documents from S3 with daily caching."""
    try:
        if MANUAL_RESEARCH_AVAILABLE:
            # DocumentStorage and S3_BUCKET already imported from dashboard package
            storage = DocumentStorage(S3_BUCKET)
            documents = storage.list_uploaded_documents(days=365)  # Get documents from the last year
            return documents if documents else []
        return []
    except Exception:
        return []


def _load_document_content(doc_s3_key: str) -> str:
    """Load a specific document's content from S3."""
    try:
        if MANUAL_RESEARCH_AVAILABLE:
            # DocumentStorage and S3_BUCKET already imported from dashboard package
            storage = DocumentStorage(S3_BUCKET)
            content, error = storage.get_document_content(doc_s3_key)
            if error:
                return f"[Error loading document: {error}]"
            return content
        return ""
    except Exception as e:
        return f"[Error loading document: {str(e)}]"


# =============================================================================
# AI REPORT STORAGE
# =============================================================================

def _get_reports_s3_client():
    """Get S3 client for report storage."""
    try:
        import boto3
        return boto3.client(
            's3',
            aws_access_key_id=st.secrets['aws']['access_key_id'],
            aws_secret_access_key=st.secrets['aws']['secret_access_key'],
            region_name=st.secrets['aws'].get('region', 'us-west-2')
        )
    except Exception:
        return None


def _save_ai_report(question: str, answer: str, model_type: str, data_sources: list) -> bool:
    """
    Save an AI analysis report to S3.

    Args:
        question: The user's question
        answer: The AI-generated answer
        model_type: 'fast' or 'deep'
        data_sources: List of data sources used

    Returns:
        True if saved successfully, False otherwise
    """
    s3_client = _get_reports_s3_client()
    if not s3_client:
        return False

    try:
        import json
        from datetime import datetime
        import hashlib

        timestamp = datetime.now()

        # Generate a unique report ID
        report_id = hashlib.md5(f"{timestamp.isoformat()}{question[:50]}".encode()).hexdigest()[:12]

        report = {
            'report_id': report_id,
            'timestamp': timestamp.isoformat(),
            'date': timestamp.strftime('%Y-%m-%d'),
            'time': timestamp.strftime('%H:%M:%S'),
            'question': question,
            'answer': answer,
            'model_type': model_type,
            'model_name': 'Claude Opus (Deep Insights)' if model_type == 'deep' else 'Claude Sonnet (Fast Insights)',
            'data_sources': data_sources
        }

        # Save to S3 with date-based path
        bucket = st.secrets['aws'].get('bucket_name', 'retail-data-bcgr')
        s3_key = f"ai-reports/{timestamp.strftime('%Y/%m')}/{report_id}.json"

        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=json.dumps(report, indent=2, default=str),
            ContentType='application/json'
        )

        return True
    except Exception as e:
        print(f"Error saving report: {e}")
        return False


def _load_ai_reports(limit: int = 50) -> list:
    """
    Load recent AI reports from S3.

    Args:
        limit: Maximum number of reports to load

    Returns:
        List of report dictionaries, sorted by date (newest first)
    """
    s3_client = _get_reports_s3_client()
    if not s3_client:
        return []

    try:
        import json

        bucket = st.secrets['aws'].get('bucket_name', 'retail-data-bcgr')
        prefix = 'ai-reports/'

        # List all report files
        paginator = s3_client.get_paginator('list_objects_v2')
        reports = []

        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                if obj['Key'].endswith('.json'):
                    try:
                        response = s3_client.get_object(Bucket=bucket, Key=obj['Key'])
                        report = json.loads(response['Body'].read().decode('utf-8'))
                        reports.append(report)
                    except Exception:
                        continue

        # Sort by timestamp (newest first)
        reports.sort(key=lambda x: x.get('timestamp', ''), reverse=True)

        return reports[:limit]
    except Exception as e:
        print(f"Error loading reports: {e}")
        return []


def _delete_ai_report(report_id: str, timestamp: str) -> bool:
    """Delete a specific AI report from S3."""
    s3_client = _get_reports_s3_client()
    if not s3_client:
        return False

    try:
        from datetime import datetime

        # Parse timestamp to get the S3 path
        dt = datetime.fromisoformat(timestamp)
        bucket = st.secrets['aws'].get('bucket_name', 'retail-data-bcgr')
        s3_key = f"ai-reports/{dt.strftime('%Y/%m')}/{report_id}.json"

        s3_client.delete_object(Bucket=bucket, Key=s3_key)
        return True
    except Exception as e:
        print(f"Error deleting report: {e}")
        return False


def render_recommendations(state, analytics):
    """Render AI-powered recommendations page."""
    st.header("💡 Business Recommendations")

    if state.sales_data is None:
        st.warning("Please upload data to generate recommendations.")
        return

    # Create tabs for different recommendation types
    tab1, tab2 = st.tabs(["🤖 AI Analysis", "📁 Past Reports"])

    with tab1:
        # Claude AI Integration (imported from dashboard package)
        if not CLAUDE_AVAILABLE:
            st.warning("Claude integration module not found. Check the dashboard package installation.")
            return

        # Get API key from environment variable or secrets
        api_key = os.environ.get("ANTHROPIC_API_KEY")

        # Fallback to secrets if environment variable not set
        if not api_key:
            try:
                # Try root level first
                api_key = st.secrets["ANTHROPIC_API_KEY"]
            except Exception:
                pass

        # Try nested anthropic section if still not found
        if not api_key:
            try:
                api_key = st.secrets["anthropic"]["ANTHROPIC_API_KEY"]
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

        # =============================================================================
        # SMART CACHED DATA LOADING - Hash-based cache invalidation
        # Only reloads data when the underlying data has actually changed
        # =============================================================================

        @st.cache_data(ttl=86400, show_spinner=False)  # 24-hour TTL for fingerprint check (use manual refresh for immediate updates)
        def _get_data_fingerprint():
            """
            Get a lightweight fingerprint of current data state.
            This is fast - just counts records, doesn't load full data.
            Used to detect if data has changed and cache should be invalidated.
            """
            fingerprint = {'invoice_count': 0, 'research_updated': '', 'seo_updated': ''}
            try:
                if INVOICE_DATA_AVAILABLE:
                    aws_config = {
                        'aws_access_key': st.secrets['aws']['access_key_id'],
                        'aws_secret_key': st.secrets['aws']['secret_access_key'],
                        'region': st.secrets['aws']['region']
                    }
                    invoice_svc = InvoiceDataService(**aws_config)
                    # Quick count query - much faster than full scan
                    invoices_table = invoice_svc.dynamodb.Table(invoice_svc.invoices_table_name)
                    response = invoices_table.scan(Select='COUNT')
                    fingerprint['invoice_count'] = response.get('Count', 0)
            except Exception:
                pass
            return _compute_data_hash(fingerprint)

        @st.cache_data(ttl=86400, show_spinner=False)  # Cache for 24 hours (use manual refresh for immediate updates)
        def _load_invoice_data_cached():
            """Load invoice data from DynamoDB with 24-hour cache."""
            if not INVOICE_DATA_AVAILABLE:
                return None, None
            try:
                aws_config = {
                    'aws_access_key': st.secrets['aws']['access_key_id'],
                    'aws_secret_key': st.secrets['aws']['secret_access_key'],
                    'region': st.secrets['aws']['region']
                }
                invoice_svc = InvoiceDataService(**aws_config)
                inv_summary = invoice_svc.get_invoice_summary()
                prod_summary = invoice_svc.get_product_summary()
                return inv_summary, prod_summary
            except Exception:
                return None, None

        @st.cache_data(ttl=86400, show_spinner=False)  # Cache for 24 hours (use manual refresh for immediate updates)
        def _load_research_data_cached():
            """Load research data from S3 with 24-hour cache.

            Tries multiple sources:
            1. Automated research findings (research-findings/summary/latest.json)
            2. Manual research monthly summaries (most recent)
            """
            if not RESEARCH_AVAILABLE:
                return None

            # First, try automated research findings
            try:
                research_viewer = ResearchFindingsViewer()
                if research_viewer.is_available():
                    automated_summary = research_viewer.load_latest_summary()
                    if automated_summary:
                        return automated_summary
            except Exception:
                pass

            # Fall back to manual research summaries if available
            if MANUAL_RESEARCH_AVAILABLE:
                try:
                    api_key = os.environ.get("ANTHROPIC_API_KEY")
                    if not api_key:
                        try:
                            api_key = st.secrets.get("ANTHROPIC_API_KEY")
                        except Exception:
                            pass
                    if not api_key:
                        try:
                            api_key = st.secrets.get("anthropic", {}).get("ANTHROPIC_API_KEY")
                        except Exception:
                            pass

                    if api_key:
                        summarizer = MonthlyResearchSummarizer(api_key)
                        # Get most recent monthly summary
                        manual_summary = summarizer.recall_summary()
                        if manual_summary:
                            # Convert to format expected by recommendations tab
                            return {
                                'executive_summary': manual_summary.get('executive_summary', ''),
                                'key_findings': [
                                    {
                                        'finding': insight.get('insight', ''),
                                        'importance': insight.get('importance', 'medium'),
                                        'category': insight.get('category', 'other'),
                                        'status': 'new',
                                        'first_identified': manual_summary.get('generated_at', '')[:10]
                                    }
                                    for insight in manual_summary.get('key_insights', [])
                                ],
                                'action_items': [
                                    {
                                        'action': action.get('action', ''),
                                        'priority': action.get('priority', 'medium'),
                                        'deadline': action.get('timeline', 'Ongoing')
                                    }
                                    for insight in manual_summary.get('key_insights', [])
                                    for action in insight.get('recommended_actions', [])
                                ],
                                'tracking_items': [
                                    {
                                        'item': risk.get('risk', ''),
                                        'severity': risk.get('severity', 'medium'),
                                        'mitigation': risk.get('mitigation', '')
                                    }
                                    for risk in manual_summary.get('risks_and_challenges', [])
                                ],
                                'generated_at': manual_summary.get('generated_at', ''),
                                'source': 'manual_research',
                                'month_name': manual_summary.get('month_name', ''),
                                'documents_analyzed': manual_summary.get('documents_analyzed', 0)
                            }
                except Exception:
                    pass

            return None

        @st.cache_data(ttl=86400, show_spinner=False)  # Cache for 24 hours (use manual refresh for immediate updates)
        def _load_seo_data_cached():
            """Load SEO data from S3 with 24-hour cache."""
            if not SEO_AVAILABLE:
                return {}
            try:
                seo_data_dict = {}
                for site_name, site_url in [("Barbary Coast", "https://barbarycoastsf.com"), ("Grass Roots", "https://grassrootssf.com")]:
                    seo_viewer = SEOFindingsViewer(website=site_url)
                    if seo_viewer.is_available():
                        seo_data = seo_viewer.load_latest_summary()
                        if seo_data:
                            seo_data_dict[site_name] = seo_data
                return seo_data_dict
            except Exception:
                return {}

        # Initialize session state cache keys if not present
        if 'recommendations_invoice_summary' not in st.session_state:
            st.session_state.recommendations_invoice_summary = None
        if 'recommendations_product_summary' not in st.session_state:
            st.session_state.recommendations_product_summary = None
        if 'recommendations_research_summary' not in st.session_state:
            st.session_state.recommendations_research_summary = None
        if 'recommendations_seo_summaries' not in st.session_state:
            st.session_state.recommendations_seo_summaries = None
        if 'recommendations_data_loaded' not in st.session_state:
            st.session_state.recommendations_data_loaded = False
        if 'recommendations_data_hash' not in st.session_state:
            st.session_state.recommendations_data_hash = None

        # Load data only once per session (or when refresh is requested)
        invoice_service = None
        invoice_summary = None
        product_purchase_summary = None
        research_summary = None
        seo_summaries = {}

        # Check if we need to load data
        needs_refresh = not st.session_state.recommendations_data_loaded

        # Smart hash-based invalidation: check if data has changed
        if st.session_state.recommendations_data_loaded:
            current_hash = _get_data_fingerprint()
            if current_hash != st.session_state.recommendations_data_hash:
                # Show loading overlay BEFORE invalidating cache
                _show_loading_overlay("Syncing data...", "New data detected in cloud")
                # Data has changed - invalidate cache
                needs_refresh = True
                st.session_state.recommendations_data_loaded = False
                _load_invoice_data_cached.clear()
                _load_research_data_cached.clear()
                _load_seo_data_cached.clear()

        # Add refresh button in sidebar for manual data refresh
        with st.sidebar:
            if st.button("🔄 Refresh Data", key="refresh_recommendations_data", help="Reload all data from sources"):
                # Show loading overlay for manual refresh
                _show_loading_overlay("Refreshing data...", "Fetching latest data from cloud")
                # Clear all global caches for data consistency
                _clear_all_data_caches()
                # Also clear local recommendation page caches
                _get_data_fingerprint.clear()
                _load_invoice_data_cached.clear()
                _load_research_data_cached.clear()
                _load_seo_data_cached.clear()
                needs_refresh = True

        if needs_refresh:
            with st.spinner("Loading data sources..."):
                # Load all data using cached functions (24-hour TTL)
                # These functions cache their results, so subsequent calls are instant
                inv_data = _load_invoice_data_cached()
                st.session_state.recommendations_invoice_summary = inv_data[0] if inv_data else None
                st.session_state.recommendations_product_summary = inv_data[1] if inv_data else None

                st.session_state.recommendations_research_summary = _load_research_data_cached()
                st.session_state.recommendations_seo_summaries = _load_seo_data_cached()

                # Store the current data hash for future comparisons
                st.session_state.recommendations_data_hash = _get_data_fingerprint()
                st.session_state.recommendations_data_loaded = True

                # Save to localStorage for persistence across browser sessions (includes hash for validation)
                cache_data = {
                    'invoice_summary': st.session_state.recommendations_invoice_summary,
                    'product_summary': st.session_state.recommendations_product_summary,
                    'research_summary': st.session_state.recommendations_research_summary,
                    'seo_summaries': st.session_state.recommendations_seo_summaries,
                    'data_hash': st.session_state.recommendations_data_hash,
                    'cached_at': datetime.now().isoformat()
                }
                _save_to_localstorage('recommendations_data', cache_data)
                # Also save the hash separately for quick validation checks
                _save_hash_to_localstorage(st.session_state.recommendations_data_hash)

        # Use cached data from session state
        invoice_summary = st.session_state.recommendations_invoice_summary
        product_purchase_summary = st.session_state.recommendations_product_summary
        research_summary = st.session_state.recommendations_research_summary
        seo_summaries = st.session_state.recommendations_seo_summaries or {}

        # Display connection status (using cached values)
        if invoice_summary and invoice_summary.get('total_invoices', 0) > 0:
            st.success(f"✅ Invoice data ({invoice_summary.get('total_invoices', 0)} invoices, {product_purchase_summary.get('total_items', 0) if product_purchase_summary else 0} line items)")
        else:
            st.caption("💡 Invoice data not available")

        if research_summary:
            findings_count = len(research_summary.get('key_findings', []))
            st.success(f"✅ Research data ({findings_count} key findings)")
        else:
            st.caption("💡 Research data not available")

        if seo_summaries:
            st.success(f"✅ SEO data ({len(seo_summaries)} sites)")
        else:
            st.caption("💡 SEO data not available")

        # Get brand-product mapping
        brand_product_mapping = state.brand_product_mapping or {}
        mapping_count = len(brand_product_mapping)

        if mapping_count > 0:
            st.info(f"🔗 Using {mapping_count} brand-product mappings for enhanced analysis")
        else:
            st.caption("💡 Tip: Set up Brand-Product Mappings for more detailed category insights")

        # Calculate store metrics for Claude context
        metrics = analytics.calculate_store_metrics(state.sales_data)

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
        
        # Prepare customer data summary if available
        customer_summary = {}
        if state.customer_data is not None:
            df = state.customer_data
            customer_summary = {
                'total_customers': len(df),
                'total_lifetime_value': float(df['Lifetime Net Sales'].sum()) if 'Lifetime Net Sales' in df.columns else 0,
                'avg_lifetime_value': float(df['Lifetime Net Sales'].mean()) if 'Lifetime Net Sales' in df.columns else 0,
                'avg_order_value': float(df['Lifetime Avg Order Value'].mean()) if 'Lifetime Avg Order Value' in df.columns else 0,
                'segments': df['Customer Segment'].value_counts().to_dict() if 'Customer Segment' in df.columns else {},
                'recency_segments': df['Recency Segment'].value_counts().to_dict() if 'Recency Segment' in df.columns else {},
                'avg_age': float(df['Age'].mean()) if 'Age' in df.columns else None,
                'gender_distribution': df['Gender'].value_counts().to_dict() if 'Gender' in df.columns else {},
                'avg_days_since_visit': float(df['Days Since Last Visit'].mean()) if 'Days Since Last Visit' in df.columns else None,
                'active_customers_30d': int((df['Days Since Last Visit'] <= 30).sum()) if 'Days Since Last Visit' in df.columns else 0,
                'at_risk_customers': int((df['Days Since Last Visit'] > 90).sum()) if 'Days Since Last Visit' in df.columns else 0,
            }

        # AI Analysis Buttons
        st.markdown("**Analysis Options:**")

        # Row 1: Core analytics
        col1, col2, col3, col4 = st.columns(4)

        # Initialize session state for AI analysis result
        if 'ai_analysis_result' not in st.session_state:
            st.session_state.ai_analysis_result = None
            st.session_state.ai_analysis_title = None

        with col1:
            if st.button("📊 Analyze Sales Trends", width='stretch'):
                with st.spinner("Claude is analyzing your sales data..."):
                    analysis = claude.analyze_sales_trends(sales_summary)
                    st.session_state.ai_analysis_title = "📊 Sales Analysis"
                    st.session_state.ai_analysis_result = analysis
                    st.rerun()

        with col2:
            if st.button("🏷️ Brand Recommendations", width='stretch'):
                if not brand_summary:
                    st.warning("Upload brand data first.")
                else:
                    with st.spinner("Claude is analyzing brand performance..."):
                        analysis = claude.analyze_brand_performance(brand_summary, brand_by_category)
                        st.session_state.ai_analysis_title = "🏷️ Brand Analysis"
                        st.session_state.ai_analysis_result = analysis
                        st.rerun()

        with col3:
            if st.button("📦 Category Insights", width='stretch'):
                if not brand_by_category:
                    st.warning("Set up brand-product mappings first to get category insights.")
                else:
                    with st.spinner("Claude is analyzing category performance..."):
                        analysis = claude.analyze_category_performance(brand_by_category, brand_summary)
                        st.session_state.ai_analysis_title = "📦 Category Analysis"
                        st.session_state.ai_analysis_result = analysis
                        st.rerun()

        with col4:
            if st.button("🎯 Deal Suggestions", width='stretch'):
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

        # Row 2: Customer analytics (if customer data available)
        if customer_summary:
            col5, col6, col7 = st.columns([1, 1, 1])

            with col5:
                if st.button("👥 Customer Insights", width='stretch'):
                    with st.spinner("Claude is analyzing customer segments..."):
                        analysis = claude.analyze_customer_segments(customer_summary, sales_summary)
                        st.session_state.ai_analysis_title = "👥 Customer Analysis"
                        st.session_state.ai_analysis_result = analysis
                        st.rerun()

            with col6:
                if st.button("🔄 Integrated Analysis", width='stretch'):
                    with st.spinner("Claude is generating integrated insights..."):
                        analysis = claude.generate_integrated_insights(
                            sales_summary,
                            customer_summary,
                            brand_summary if brand_summary else None
                        )
                        st.session_state.ai_analysis_title = "🔄 Integrated Business Insights"
                        st.session_state.ai_analysis_result = analysis
                        st.rerun()
        else:
            st.caption("💡 Upload customer data to unlock customer analytics and integrated insights")

        # Row 3: Invoice/Purchasing analytics (if invoice data available)
        if invoice_summary and invoice_summary.get('total_invoices', 0) > 0:
            st.markdown("**Purchasing Analytics:**")
            inv_col1, inv_col2, inv_col3 = st.columns([1, 1, 1])

            with inv_col1:
                if st.button("🏭 Vendor Analysis", width='stretch'):
                    with st.spinner("Claude is analyzing vendor spending..."):
                        vendor_context = {
                            'total_invoices': invoice_summary.get('total_invoices', 0),
                            'total_spend': invoice_summary.get('total_value', 0),
                            'avg_invoice': invoice_summary.get('avg_invoice_value', 0),
                            'vendors': invoice_summary.get('vendors', {})
                        }
                        prompt = f"""Analyze this vendor spending data for a cannabis dispensary:

{json.dumps(vendor_context, indent=2, default=str)}

Provide insights on:
1. Top vendors by spend and invoice frequency
2. Spending concentration (are we too dependent on one vendor?)
3. Recommendations for vendor negotiations based on spend patterns
4. Any concerning patterns or opportunities

Keep analysis concise and actionable."""

                        try:
                            message = claude.client.messages.create(
                                model=claude.model,
                                max_tokens=2000,
                                messages=[{"role": "user", "content": prompt}]
                            )
                            analysis = message.content[0].text
                        except Exception as e:
                            analysis = f"Error analyzing vendor spending: {str(e)}"

                        st.session_state.ai_analysis_title = "🏭 Vendor Spending Analysis"
                        st.session_state.ai_analysis_result = analysis
                        st.rerun()

            with inv_col2:
                if st.button("📦 Purchase Patterns", width='stretch'):
                    with st.spinner("Claude is analyzing purchase patterns..."):
                        # Get top purchased brands and product types
                        purchase_brands = product_purchase_summary.get('brands', {})
                        purchase_types = product_purchase_summary.get('product_types', {})

                        top_brands = dict(sorted(
                            purchase_brands.items(),
                            key=lambda x: x[1].get('total_cost', 0),
                            reverse=True
                        )[:15])

                        purchase_context = {
                            'top_brands': top_brands,
                            'product_types': purchase_types,
                            'total_products': product_purchase_summary.get('total_items', 0)
                        }

                        prompt = f"""Analyze this cannabis product purchasing data:

{json.dumps(purchase_context, indent=2, default=str)}

Provide insights on:
1. Top performing brands by purchase volume and spend
2. Product category breakdown (concentrates vs flower vs edibles vs cartridges)
3. Inventory recommendations - which brands/types to stock more/less of
4. Trends and opportunities for product mix optimization

Keep analysis practical and data-driven."""

                        try:
                            message = claude.client.messages.create(
                                model=claude.model,
                                max_tokens=2000,
                                messages=[{"role": "user", "content": prompt}]
                            )
                            analysis = message.content[0].text
                        except Exception as e:
                            analysis = f"Error analyzing purchase patterns: {str(e)}"

                        st.session_state.ai_analysis_title = "📦 Purchase Pattern Analysis"
                        st.session_state.ai_analysis_result = analysis
                        st.rerun()

            with inv_col3:
                if st.button("💰 Margin Optimization", width='stretch'):
                    with st.spinner("Claude is analyzing pricing and margins..."):
                        # Combine sales data with purchase data for margin analysis
                        purchase_types = product_purchase_summary.get('product_types', {})

                        # Calculate average unit costs
                        type_pricing = {}
                        for ptype, data in purchase_types.items():
                            total_units = data.get('total_units', 0)
                            avg_unit_cost = data.get('total_cost', 0) / total_units if total_units > 0 else 0
                            type_pricing[ptype] = {
                                'avg_unit_cost': round(avg_unit_cost, 2),
                                'total_units': total_units,
                                'total_cost': data.get('total_cost', 0)
                            }

                        margin_context = {
                            'product_type_pricing': type_pricing,
                            'sales_summary': sales_summary
                        }

                        prompt = f"""Analyze this wholesale pricing and sales data for cannabis products:

{json.dumps(margin_context, indent=2, default=str)}

Provide insights on:
1. Average unit costs by product type
2. Which product types have the most favorable wholesale pricing
3. Compare wholesale costs to sales performance (if sales data available)
4. Recommendations for margin optimization and pricing strategy
5. Products to prioritize for better margins

Focus on actionable pricing and margin insights."""

                        try:
                            message = claude.client.messages.create(
                                model=claude.model,
                                max_tokens=2000,
                                messages=[{"role": "user", "content": prompt}]
                            )
                            analysis = message.content[0].text
                        except Exception as e:
                            analysis = f"Error analyzing margins: {str(e)}"

                        st.session_state.ai_analysis_title = "💰 Margin Optimization Analysis"
                        st.session_state.ai_analysis_result = analysis
                        st.rerun()
        else:
            st.caption("💡 Upload invoices to unlock purchasing analytics (vendor analysis, purchase patterns, margin optimization)")

        # Row 4: Research & SEO analytics (if data available)
        if research_summary or seo_summaries:
            st.markdown("**Strategic Analytics:**")
            strat_col1, strat_col2 = st.columns([1, 1])

            with strat_col1:
                if research_summary:
                    if st.button("🔬 Industry Insights", width='stretch'):
                        with st.spinner("Claude is analyzing industry research..."):
                            research_context = {
                                'executive_summary': research_summary.get('executive_summary', ''),
                                'key_findings': research_summary.get('key_findings', []),
                                'action_items': research_summary.get('action_items', []),
                                'tracking_items': research_summary.get('tracking_items', [])
                            }

                            prompt = f"""Analyze this cannabis industry research for a San Francisco dispensary:

{json.dumps(research_context, indent=2, default=str)}

Provide strategic insights on:
1. Key industry trends that affect our business
2. Regulatory updates and compliance requirements
3. Competitive landscape changes
4. Market opportunities to capitalize on
5. Risks to monitor and mitigate
6. Specific action items prioritized by impact

Focus on actionable recommendations for Barbary Coast and Grass Roots dispensaries."""

                            try:
                                message = claude.client.messages.create(
                                    model=claude.model,
                                    max_tokens=2500,
                                    messages=[{"role": "user", "content": prompt}]
                                )
                                analysis = message.content[0].text
                            except Exception as e:
                                analysis = f"Error analyzing research: {str(e)}"

                            st.session_state.ai_analysis_title = "🔬 Industry Research Insights"
                            st.session_state.ai_analysis_result = analysis
                            st.rerun()
                else:
                    st.caption("💡 Upload research documents to unlock industry insights")

            with strat_col2:
                if seo_summaries:
                    if st.button("🔍 SEO Analysis", width='stretch'):
                        with st.spinner("Claude is analyzing SEO performance..."):
                            seo_context = {}
                            for site_name, seo_data in seo_summaries.items():
                                seo_context[site_name] = {
                                    'overall_score': seo_data.get('overall_score'),
                                    'top_priorities': seo_data.get('top_priorities', []),
                                    'quick_wins': seo_data.get('quick_wins', []),
                                    'categories': seo_data.get('categories', {}),
                                    'competitive_insights': seo_data.get('competitive_insights', '')
                                }

                            prompt = f"""Analyze this SEO data for two cannabis dispensary websites:

{json.dumps(seo_context, indent=2, default=str)}

Provide strategic SEO recommendations:
1. Overall SEO health assessment for each site
2. Critical issues to fix immediately (quick wins)
3. Technical SEO improvements needed
4. Local SEO optimization for San Francisco market
5. Content strategy recommendations
6. Comparison between the two sites - which needs more attention?
7. Prioritized action plan with expected impact

Focus on actionable improvements that will drive more organic traffic and local visibility."""

                            try:
                                message = claude.client.messages.create(
                                    model=claude.model,
                                    max_tokens=2500,
                                    messages=[{"role": "user", "content": prompt}]
                                )
                                analysis = message.content[0].text
                            except Exception as e:
                                analysis = f"Error analyzing SEO: {str(e)}"

                            st.session_state.ai_analysis_title = "🔍 SEO Performance Analysis"
                            st.session_state.ai_analysis_result = analysis
                            st.rerun()
                else:
                    st.caption("💡 Run SEO analysis to unlock website insights")

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

        # Show data availability summary
        data_sources = []
        if state.sales_data is not None:
            data_sources.append(f"📊 Sales ({len(state.sales_data):,} records)")
        if state.brand_data is not None:
            data_sources.append(f"🏷️ Brands ({len(state.brand_data):,} brands)")
        if state.customer_data is not None:
            data_sources.append(f"👥 Customers ({len(state.customer_data):,} customers)")
        if invoice_summary and invoice_summary.get('total_invoices', 0) > 0:
            data_sources.append(f"📦 Invoices ({invoice_summary['total_invoices']:,} invoices)")
        if product_purchase_summary and product_purchase_summary.get('total_items', 0) > 0:
            data_sources.append(f"🛒 Purchases ({product_purchase_summary['total_items']:,} line items)")
        if research_summary:
            findings_count = len(research_summary.get('key_findings', []))
            data_sources.append(f"🔬 Research ({findings_count} findings)")
        if seo_summaries:
            data_sources.append(f"🔍 SEO ({len(seo_summaries)} sites)")

        if data_sources:
            st.caption(f"**Available data:** {' | '.join(data_sources)}")

        # Document selector for specific research document queries
        selected_doc_contents = {}
        if MANUAL_RESEARCH_AVAILABLE:
            try:
                aws_access_key = st.secrets['aws']['access_key_id']
                aws_secret_key = st.secrets['aws']['secret_access_key']
                aws_region = st.secrets['aws'].get('region', 'us-west-2')

                research_documents = _fetch_research_documents_cached(aws_access_key, aws_secret_key, aws_region)

                if research_documents:
                    with st.expander(f"📄 Reference Specific Documents ({len(research_documents)} available)", expanded=False):
                        st.caption("Select documents to include in your query for targeted analysis")

                        # Group documents by category
                        docs_by_category = {}
                        for doc in research_documents:
                            category = doc.get('category', 'Other')
                            if category not in docs_by_category:
                                docs_by_category[category] = []
                            docs_by_category[category].append(doc)

                        # Create multiselect with formatted document names
                        doc_options = []
                        doc_map = {}  # Maps display name to doc data
                        for category, docs in sorted(docs_by_category.items()):
                            for doc in docs:
                                filename = doc.get('original_filename', 'Unknown')
                                uploaded = doc.get('uploaded_at', '')[:10]  # Just the date
                                display_name = f"[{category}] {filename} ({uploaded})"
                                doc_options.append(display_name)
                                doc_map[display_name] = doc

                        selected_docs = st.multiselect(
                            "Select documents:",
                            options=doc_options,
                            default=[],
                            help="Select one or more documents to include in your query"
                        )

                        if selected_docs:
                            st.info(f"📎 {len(selected_docs)} document(s) will be included in your query")
                            # Load content for selected documents
                            for display_name in selected_docs:
                                doc = doc_map[display_name]
                                s3_key = doc.get('s3_key', '')
                                if s3_key:
                                    content = _load_document_content(s3_key)
                                    selected_doc_contents[doc.get('original_filename', 'Unknown')] = {
                                        'content': content[:50000],  # Limit to 50k chars per doc
                                        'category': doc.get('category', 'Other'),
                                        'source_url': doc.get('source_url', ''),
                                        'uploaded_at': doc.get('uploaded_at', '')
                                    }
            except Exception as e:
                st.caption(f"💡 Document selection not available: {str(e)[:50]}")

        # Model selection toggle
        col_q, col_toggle = st.columns([4, 1])
        with col_q:
            question = st.text_input(
                "Ask anything about your sales, brands, customers, invoices, purchasing, industry research, SEO, or business strategy:",
                key="ai_analysis_question"
            )
        with col_toggle:
            use_deep_thinking = st.toggle(
                "Deep Insights",
                value=False,
                help="**OFF - Fast Insights (Sonnet)**: ~$0.10/query, quick answers\n\n**ON - Deep Insights (Opus)**: ~$2/query, strategic analysis with extended thinking"
            )

        # Submit button to trigger analysis (prevents auto-rerun on tab switch)
        submit_col1, submit_col2 = st.columns([1, 4])
        with submit_col1:
            submit_button = st.button(
                "🚀 Analyze" if not use_deep_thinking else "🧠 Deep Analyze",
                type="primary",
                disabled=not question,
                key="ai_analysis_submit"
            )

        # Only run analysis when button is clicked (not on every rerun)
        if submit_button and question:
            # Prepare comprehensive context with ALL available data sources
            context = {
                'sales_summary': sales_summary,
                'top_brands': brand_summary[:20] if brand_summary else [],
                'product_mix': state.product_data.to_dict('records') if state.product_data is not None else [],
                'brand_by_category': brand_by_category if brand_by_category else [],
                'brand_product_mapping_sample': dict(list(brand_product_mapping.items())[:30]) if brand_product_mapping else {},
                'customer_summary': customer_summary if customer_summary else {}
            }

            # Add invoice/purchasing data from DynamoDB if available
            if invoice_summary and invoice_summary.get('total_invoices', 0) > 0:
                context['invoice_summary'] = {
                    'total_invoices': invoice_summary.get('total_invoices', 0),
                    'total_purchase_value': invoice_summary.get('total_value', 0),
                    'avg_invoice_value': invoice_summary.get('avg_invoice_value', 0),
                    'vendors': invoice_summary.get('vendors', {})
                }

            if product_purchase_summary and product_purchase_summary.get('total_items', 0) > 0:
                # Include top purchased brands and product types
                purchase_brands = product_purchase_summary.get('brands', {})
                purchase_types = product_purchase_summary.get('product_types', {})

                # Sort and limit to top entries for context efficiency
                top_purchase_brands = dict(sorted(
                    purchase_brands.items(),
                    key=lambda x: x[1].get('total_cost', 0),
                    reverse=True
                )[:20])

                context['purchase_data'] = {
                    'total_line_items': product_purchase_summary.get('total_items', 0),
                    'top_purchased_brands': top_purchase_brands,
                    'product_types': purchase_types
                }

            # Add research findings if available
            if research_summary:
                context['research_findings'] = {
                    'executive_summary': research_summary.get('executive_summary', ''),
                    'key_findings': research_summary.get('key_findings', [])[:10],  # Top 10 findings
                    'action_items': research_summary.get('action_items', [])[:5],  # Top 5 action items
                    'tracking_items': research_summary.get('tracking_items', [])[:5]  # Items being tracked
                }

            # Add SEO analysis if available
            if seo_summaries:
                seo_context = {}
                for site_name, seo_data in seo_summaries.items():
                    seo_context[site_name] = {
                        'overall_score': seo_data.get('overall_score'),
                        'top_priorities': seo_data.get('top_priorities', [])[:5],
                        'quick_wins': seo_data.get('quick_wins', [])[:5],
                        'categories': {cat: {'score': data.get('score')} for cat, data in seo_data.get('categories', {}).items() if isinstance(data, dict)}
                    }
                context['seo_analysis'] = seo_context

            # Add selected research documents for targeted queries
            if selected_doc_contents:
                context['selected_research_documents'] = selected_doc_contents

            # Show appropriate spinner based on model
            spinner_text = "🧠 Generating Deep Insights..." if use_deep_thinking else "⚡ Generating Fast Insights..."
            with st.spinner(spinner_text):
                answer = claude.answer_business_question(question, context, use_deep_thinking=use_deep_thinking)

                # Save the report to S3
                model_type = 'deep' if use_deep_thinking else 'fast'
                report_saved = _save_ai_report(
                    question=question,
                    answer=answer,
                    model_type=model_type,
                    data_sources=data_sources
                )

                # Store result in session state so it persists across tab switches
                st.session_state.ai_analysis_last_result = {
                    'question': question,
                    'answer': answer,
                    'model_type': model_type,
                    'report_saved': report_saved
                }

        # Display the last result (either just generated or from session state)
        if 'ai_analysis_last_result' in st.session_state and st.session_state.ai_analysis_last_result:
            result = st.session_state.ai_analysis_last_result
            st.markdown("### Answer")
            if result['model_type'] == 'deep':
                st.caption("🧠 *Deep Insights - Analyzed with Claude Opus + Extended Thinking*")
            else:
                st.caption("⚡ *Fast Insights - Analyzed with Claude Sonnet*")
            st.markdown(result['answer'])

            if result.get('report_saved'):
                st.success("📁 Report saved to Past Reports")

            # Add clear button to start fresh
            if st.button("🗑️ Clear Result", key="clear_ai_result"):
                st.session_state.ai_analysis_last_result = None
                st.rerun()

    with tab2:
        # Past Reports Tab
        st.subheader("📁 Past AI Analysis Reports")
        st.caption("View and print previous AI-generated insights")

        # Refresh button
        col1, col2 = st.columns([3, 1])
        with col2:
            if st.button("🔄 Refresh", key="refresh_reports"):
                st.rerun()

        # Load reports
        with st.spinner("Loading reports..."):
            reports = _load_ai_reports(limit=50)

        if not reports:
            st.info("No saved reports yet. Generate an AI analysis in the 'AI Analysis' tab to create your first report.")
        else:
            st.success(f"Found {len(reports)} saved report(s)")

            # Filter options
            col1, col2 = st.columns(2)
            with col1:
                model_filter = st.selectbox(
                    "Filter by type:",
                    options=["All", "Deep Insights", "Fast Insights"],
                    key="report_model_filter"
                )
            with col2:
                # Get unique dates for filtering
                unique_dates = sorted(set(r.get('date', '') for r in reports), reverse=True)
                date_filter = st.selectbox(
                    "Filter by date:",
                    options=["All"] + unique_dates,
                    key="report_date_filter"
                )

            # Apply filters
            filtered_reports = reports
            if model_filter == "Deep Insights":
                filtered_reports = [r for r in filtered_reports if r.get('model_type') == 'deep']
            elif model_filter == "Fast Insights":
                filtered_reports = [r for r in filtered_reports if r.get('model_type') == 'fast']

            if date_filter != "All":
                filtered_reports = [r for r in filtered_reports if r.get('date') == date_filter]

            st.markdown(f"**Showing {len(filtered_reports)} report(s)**")
            st.markdown("---")

            # Display reports
            for i, report in enumerate(filtered_reports):
                report_date = report.get('date', 'Unknown')
                report_time = report.get('time', '')
                model_name = report.get('model_name', 'Unknown')
                model_type = report.get('model_type', 'fast')
                question = report.get('question', 'No question recorded')
                answer = report.get('answer', 'No answer recorded')
                report_id = report.get('report_id', '')
                timestamp = report.get('timestamp', '')

                # Model badge
                model_badge = "🧠" if model_type == 'deep' else "⚡"

                with st.expander(f"{model_badge} {report_date} {report_time} - {question[:60]}{'...' if len(question) > 60 else ''}", expanded=False):
                    # Report header
                    st.markdown(f"**Date:** {report_date} at {report_time}")
                    st.markdown(f"**Model:** {model_name}")

                    # Data sources used
                    data_sources = report.get('data_sources', [])
                    if data_sources:
                        st.markdown(f"**Data Sources:** {' | '.join(data_sources)}")

                    st.markdown("---")

                    # Question
                    st.markdown("**Question:**")
                    st.info(question)

                    # Answer (print-friendly format)
                    st.markdown("**Analysis:**")
                    st.markdown(answer)

                    # Action buttons
                    col1, col2, col3 = st.columns([1, 1, 2])
                    with col1:
                        # Copy to clipboard button (using markdown workaround)
                        if st.button("📋 Copy", key=f"copy_{report_id}"):
                            st.code(f"Question: {question}\n\nAnalysis:\n{answer}", language=None)
                            st.caption("Select and copy the text above")

                    with col2:
                        # Delete button
                        if st.button("🗑️ Delete", key=f"delete_{report_id}"):
                            if _delete_ai_report(report_id, timestamp):
                                st.success("Report deleted")
                                st.rerun()
                            else:
                                st.error("Failed to delete report")

                    with col3:
                        # Print instructions
                        st.caption("💡 To print: Expand report → Right-click → Print")


def render_brand_product_mapping(state, s3_manager):
    """Render brand-product mapping configuration interface."""
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
                if st.button("💾 Save", key="quick_save", width='stretch'):
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
                width='stretch',
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


def render_data_center(s3_manager, processor):
    """Render Data Center page with tabbed interface for all data management features."""
    st.header("🗄️ Data Center")

    # S3 Connection Status with consolidated record counts
    s3_connected, s3_message = s3_manager.test_connection()

    # Build consolidated status message
    record_counts = []

    # Sales records
    if st.session_state.sales_data is not None:
        record_counts.append(f"Sales: {len(st.session_state.sales_data)}")

    # Brand records
    if st.session_state.brand_data is not None:
        record_counts.append(f"Brands: {len(st.session_state.brand_data)}")

    # Product records
    if st.session_state.product_data is not None:
        record_counts.append(f"Products: {len(st.session_state.product_data)}")

    # Invoice records
    if st.session_state.invoice_data is not None:
        dynamo_count = st.session_state.get('dynamo_invoice_count', 0)
        if dynamo_count > 0:
            record_counts.append(f"Invoices: {len(st.session_state.invoice_data)} items")
        else:
            record_counts.append(f"Invoices: {len(st.session_state.invoice_data)} items")

    # Customer records
    if st.session_state.customer_data is not None:
        record_counts.append(f"Customers: {len(st.session_state.customer_data)}")

    # Business context records
    context_count = 0
    if BUSINESS_CONTEXT_AVAILABLE:
        try:
            context_service = get_business_context_service()
            if context_service:
                context_count = context_service.get_context_count()
                if context_count > 0:
                    record_counts.append(f"Context: {context_count}")
        except Exception:
            pass

    # Display consolidated status
    if s3_connected:
        if record_counts:
            status_text = f"✅ {s3_message} | Records: {' | '.join(record_counts)}"
        else:
            status_text = f"✅ {s3_message} | No data loaded"
        st.success(status_text)
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

    # Create main tabs for Data Center
    # Build tab list dynamically based on available modules
    tab_names = [
        "📊 Sales Data",
        "📋 Invoice Data",
        "👥 Customer Data",
        "🎯 Budtender Performance",
        "💡 Define Context",
        "🔗 Brand Mapping",
    ]

    # Add optional tabs based on available modules
    if RESEARCH_AVAILABLE:
        tab_names.append("🔬 Industry Research")
    if SEO_AVAILABLE:
        tab_names.append("🔍 SEO Analysis")
    if QR_AVAILABLE:
        tab_names.append("📱 QR Portal")

    tabs = st.tabs(tab_names)

    # Assign tabs to variables (handle dynamic count)
    tab_idx = 0
    sales_tab = tabs[tab_idx]; tab_idx += 1
    invoice_tab = tabs[tab_idx]; tab_idx += 1
    customer_tab = tabs[tab_idx]; tab_idx += 1
    budtender_tab = tabs[tab_idx]; tab_idx += 1
    context_tab = tabs[tab_idx]; tab_idx += 1
    brand_mapping_tab = tabs[tab_idx]; tab_idx += 1

    research_tab = None
    if RESEARCH_AVAILABLE:
        research_tab = tabs[tab_idx]; tab_idx += 1

    seo_tab = None
    if SEO_AVAILABLE:
        seo_tab = tabs[tab_idx]; tab_idx += 1

    qr_tab = None
    if QR_AVAILABLE:
        qr_tab = tabs[tab_idx]; tab_idx += 1

    # =========================================================================
    # SALES DATA TAB - Sales, Brand, and Product uploads
    # =========================================================================
    with sales_tab:
        st.markdown("Upload sales reports from Treez to analyze store performance, brand trends, and product data.")

        # File uploaders in 3 columns
        col1, col2, col3 = st.columns(3)

        with col1:
            st.subheader("Sales Data")
            sales_file = st.file_uploader("Upload Sales by Store CSV", type=['csv'], key='sales_upload')

            if sales_file:
                df = pd.read_csv(sales_file)
                st.success(f"Loaded {len(df)} rows")

                # Preview
                with st.expander("Preview Data"):
                    st.dataframe(df.head(), width='stretch')

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

                # Handle column name change: Treez renamed 'Brand' to 'Product Brand' after 12/01/2025
                if 'Product Brand' in df.columns and 'Brand' not in df.columns:
                    df = df.rename(columns={'Product Brand': 'Brand'})
                    st.info("ℹ️ Detected new Treez format - 'Product Brand' column renamed to 'Brand'")

                # Validate that this is brand data (must have 'Brand' column)
                if 'Brand' not in df.columns:
                    st.error(f"⚠️ This doesn't appear to be Brand data. Expected 'Brand' or 'Product Brand' column but found: {', '.join(df.columns[:5])}...")
                    st.info("Please upload a 'Net Sales by Brand' report from Treez.")
                else:
                    # Show sample record count that will be filtered
                    sample_count = df['Brand'].str.startswith(('[DS]', '[SS]'), na=False).sum()
                    if sample_count > 0:
                        st.info(f"ℹ️ {sample_count} sample records ([DS]/[SS]) will be filtered out")

                    # Preview
                    with st.expander("Preview Data"):
                        st.dataframe(df.head(), width='stretch')

                if 'Brand' in df.columns and st.button("Process Brand Data", key="process_brand"):
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
                    st.dataframe(df.head(), width='stretch')

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

    # =========================================================================
    # INVOICE DATA TAB
    # =========================================================================
    with invoice_tab:
        # Show current invoice data status
        if st.session_state.invoice_data is not None:
            inv_col1, inv_col2 = st.columns([3, 1])
            with inv_col1:
                total_count = len(st.session_state.invoice_data)
                dynamo_count = st.session_state.get('dynamo_invoice_count', 0)
                if dynamo_count > 0:
                    st.success(f"✅ **{total_count} invoice line items loaded** ({dynamo_count} from DynamoDB)")
                else:
                    st.info(f"📊 {total_count} invoice line items loaded (from S3)")
            with inv_col2:
                if st.session_state.get('dynamo_load_error'):
                    st.error("⚠️ DynamoDB Error")
                    with st.expander("View Error"):
                        st.code(st.session_state.dynamo_load_error)
        else:
            if st.session_state.get('dynamo_load_error'):
                st.warning(f"⚠️ No invoice data loaded. DynamoDB error: {st.session_state.dynamo_load_error[:100]}")
            else:
                st.info("📤 No invoice data loaded yet. Upload invoices below to get started.")

        st.markdown("""
        **Features:**
        - 🚀 **Auto-extraction** - Parses Treez invoices without Claude API costs
        - 💾 **DynamoDB storage** - Fast, queryable invoice database
        - 🤖 **Claude analytics** - AI-powered insights on your purchasing data
        - 💰 **Cost-efficient** - Saves $50-200 per 100 invoices vs traditional extraction
        """)

        # Use the integrated invoice upload UI with all tabs (Upload, View Data, Date Review)
        # render_full_invoice_section already imported from dashboard package
        if INVOICE_UPLOAD_AVAILABLE and render_full_invoice_section:
            try:
                render_full_invoice_section()
            except Exception as e:
                st.error(f"Error rendering invoice upload section: {e}")
                import traceback
                st.code(traceback.format_exc())
        else:
            st.warning("Invoice upload module not available.")
            st.info("📦 The invoice upload module is not installed or failed to load.")

    # =========================================================================
    # CUSTOMER DATA TAB
    # =========================================================================
    with customer_tab:
        st.markdown("""
        Upload customer data to enable demographic analysis and customer segmentation insights.
        This data will be analyzed alongside sales and product data for comprehensive business intelligence.
        """)

        customer_file = st.file_uploader(
            "Upload Customer Data CSV",
            type=['csv'],
            key='customer_upload',
            help="Upload a CSV file containing customer demographic and transaction history data"
        )

        if customer_file:
            df = pd.read_csv(customer_file)
            st.success(f"✅ Loaded {len(df)} customer records")

            # Preview
            with st.expander("Preview Customer Data"):
                st.dataframe(df.head(10), width='stretch')
                st.caption(f"Columns: {', '.join(df.columns.tolist()[:10])}{'...' if len(df.columns) > 10 else ''}")

            if st.button("Process Customer Data", key="process_customer"):
                with st.spinner("Processing customer data..."):
                    # Clean and process customer data
                    processed = processor.clean_customer_data(df)

                    # Add metadata
                    processed['Upload_Store'] = store_id
                    processed['Upload_Date'] = pd.to_datetime(datetime.now())

                    # Merge with existing data or replace
                    if st.session_state.customer_data is not None:
                        # Merge by Customer ID, keeping latest version
                        customer_id_col = 'Customer ID' if 'Customer ID' in processed.columns else 'id'
                        st.session_state.customer_data = pd.concat([
                            st.session_state.customer_data,
                            processed
                        ]).drop_duplicates(subset=[customer_id_col], keep='last')
                    else:
                        st.session_state.customer_data = processed

                    # Upload to S3
                    customer_file.seek(0)
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    s3_key = f"raw-uploads/{store_id}/customers_{timestamp}.csv"

                    success, message = s3_manager.upload_file(customer_file, s3_key)
                    if success:
                        st.success(f"✅ {message}")
                    else:
                        st.warning(f"⚠️ S3 upload failed: {message}")
                        st.info("Data processed locally but NOT saved to S3")

                    # Show processing summary
                    st.success("✅ Customer data processed!")

                    # Show segments summary
                    if 'Customer Segment' in processed.columns:
                        segment_counts = processed['Customer Segment'].value_counts()
                        st.markdown("**Customer Segments:**")
                        seg_cols = st.columns(5)
                        for idx, (seg, count) in enumerate(segment_counts.items()):
                            with seg_cols[idx % 5]:
                                st.metric(seg, count)

                    if 'Recency Segment' in processed.columns:
                        recency_counts = processed['Recency Segment'].value_counts()
                        st.markdown("**Recency Segments:**")
                        rec_cols = st.columns(5)
                        for idx, (seg, count) in enumerate(recency_counts.items()):
                            with rec_cols[idx % 5]:
                                st.metric(seg, count)

                    st.rerun()

        # Customer data status
        if st.session_state.customer_data is not None:
            with st.expander("📊 Customer Data Status"):
                df = st.session_state.customer_data
                st.success(f"✅ {len(df)} customer records loaded")

                cols = st.columns(4)
                with cols[0]:
                    if 'Customer Segment' in df.columns:
                        st.caption("**Value Segments:**")
                        for seg, count in df['Customer Segment'].value_counts().items():
                            st.text(f"{seg}: {count}")

                with cols[1]:
                    if 'Recency Segment' in df.columns:
                        st.caption("**Recency:**")
                        for seg, count in df['Recency Segment'].value_counts().items():
                            st.text(f"{seg}: {count}")

                with cols[2]:
                    if 'Age' in df.columns:
                        avg_age = df['Age'].mean()
                        st.caption("**Demographics:**")
                        st.text(f"Avg Age: {avg_age:.1f}")
                        if 'Gender' in df.columns:
                            gender_dist = df['Gender'].value_counts()
                            for gender, count in gender_dist.items():
                                st.text(f"{gender}: {count}")

                with cols[3]:
                    if 'Lifetime Net Sales' in df.columns:
                        total_ltv = df['Lifetime Net Sales'].sum()
                        avg_ltv = df['Lifetime Net Sales'].mean()
                        st.caption("**Lifetime Value:**")
                        st.text(f"Total: ${total_ltv:,.0f}")
                        st.text(f"Avg: ${avg_ltv:,.0f}")

        # View uploaded customer files from S3
        with st.expander("📋 View Uploaded Customer Files"):
            if s3_connected:
                customer_files = [f for f in s3_manager.list_files(prefix="raw-uploads/") if '/customers_' in f and f.endswith('.csv')]

                if customer_files:
                    st.success(f"✅ {len(customer_files)} customer data file(s) in S3")

                    # Group by store
                    from collections import defaultdict
                    by_store = defaultdict(list)

                    for f in customer_files:
                        cust_store_id = s3_manager._extract_store_from_path(f)
                        store_name = STORE_DISPLAY_NAMES.get(cust_store_id, cust_store_id.replace('_', ' ').title())
                        by_store[store_name].append(f)

                    for store, files in sorted(by_store.items()):
                        st.markdown(f"**{store}** ({len(files)} upload(s))")
                        for f in sorted(files, reverse=True)[:5]:  # Show 5 most recent
                            filename = f.split('/')[-1]
                            # Extract timestamp from filename
                            try:
                                timestamp_str = filename.split('_')[-1].replace('.csv', '')
                                file_timestamp = datetime.strptime(timestamp_str, '%Y%m%d_%H%M%S')
                                st.text(f"  • {file_timestamp.strftime('%m/%d/%Y %I:%M %p')}")
                            except:
                                st.text(f"  • {filename}")
                        if len(files) > 5:
                            st.text(f"  ... and {len(files) - 5} more")
                else:
                    st.info("No customer data uploaded yet")
            else:
                st.warning("S3 not connected - cannot view uploaded customer files")

    # =========================================================================
    # BUDTENDER PERFORMANCE TAB
    # =========================================================================
    with budtender_tab:
        st.markdown("""
        Upload Budtender Performance Lifetime reports from Treez to track employee sales performance
        and identify top-selling products by budtender. This data helps with staff training,
        performance reviews, and understanding product sales patterns.
        """)

        # File uploaders for each store
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Barbary Coast")
            bc_budtender_file = st.file_uploader(
                "Upload BC Budtender Performance CSV",
                type=['csv'],
                key='bc_budtender_upload',
                help="BudtenderPerformanceLifetime report from Treez for Barbary Coast"
            )

            if bc_budtender_file:
                try:
                    bc_df = pd.read_csv(bc_budtender_file)
                    bc_df['Store_ID'] = 'barbary_coast'
                    st.success(f"Loaded {len(bc_df):,} records from Barbary Coast")

                    # Show preview
                    with st.expander("Preview Data", expanded=False):
                        st.dataframe(bc_df.head(20), height=300)

                    # Show summary stats
                    if 'Employee' in bc_df.columns:
                        unique_employees = bc_df['Employee'].nunique()
                        unique_brands = bc_df['Product Brand'].nunique() if 'Product Brand' in bc_df.columns else 0
                        total_units = bc_df['Units Sold'].sum() if 'Units Sold' in bc_df.columns else 0
                        total_sales = bc_df['Net Sales'].sum() if 'Net Sales' in bc_df.columns else 0

                        st.markdown(f"""
                        **Summary:**
                        - **Budtenders:** {unique_employees}
                        - **Brands Sold:** {unique_brands}
                        - **Total Units:** {total_units:,.0f}
                        - **Total Net Sales:** ${total_sales:,.2f}
                        """)
                except Exception as e:
                    st.error(f"Error reading file: {str(e)}")
                    bc_df = None
            else:
                bc_df = None

        with col2:
            st.subheader("Grass Roots")
            gr_budtender_file = st.file_uploader(
                "Upload GR Budtender Performance CSV",
                type=['csv'],
                key='gr_budtender_upload',
                help="BudtenderPerformanceLifetime report from Treez for Grass Roots"
            )

            if gr_budtender_file:
                try:
                    gr_df = pd.read_csv(gr_budtender_file)
                    gr_df['Store_ID'] = 'grass_roots'
                    st.success(f"Loaded {len(gr_df):,} records from Grass Roots")

                    # Show preview
                    with st.expander("Preview Data", expanded=False):
                        st.dataframe(gr_df.head(20), height=300)

                    # Show summary stats
                    if 'Employee' in gr_df.columns:
                        unique_employees = gr_df['Employee'].nunique()
                        unique_brands = gr_df['Product Brand'].nunique() if 'Product Brand' in gr_df.columns else 0
                        total_units = gr_df['Units Sold'].sum() if 'Units Sold' in gr_df.columns else 0
                        total_sales = gr_df['Net Sales'].sum() if 'Net Sales' in gr_df.columns else 0

                        st.markdown(f"""
                        **Summary:**
                        - **Budtenders:** {unique_employees}
                        - **Brands Sold:** {unique_brands}
                        - **Total Units:** {total_units:,.0f}
                        - **Total Net Sales:** ${total_sales:,.2f}
                        """)
                except Exception as e:
                    st.error(f"Error reading file: {str(e)}")
                    gr_df = None
            else:
                gr_df = None

        # Combine and save button
        st.markdown("---")

        if bc_df is not None or gr_df is not None:
            # Combine dataframes
            dfs_to_combine = []
            if bc_df is not None:
                dfs_to_combine.append(bc_df)
            if gr_df is not None:
                dfs_to_combine.append(gr_df)

            combined_df = pd.concat(dfs_to_combine, ignore_index=True)

            # Standardize column names
            column_mapping = {
                'Store Name': 'Store_Name',
                'Product Brand': 'Product_Brand',
                'Units Sold': 'Units_Sold',
                'Store Average': 'Store_Avg_Units',
                'Discount %': 'Discount_Pct',
                'Store Average.1': 'Store_Avg_Discount',
                'Net Sales': 'Net_Sales',
                'Store Average.2': 'Store_Avg_Sales',
                'Gross  Margin ': 'Gross_Margin',
                'Store Average.3': 'Store_Avg_Margin'
            }
            combined_df = combined_df.rename(columns=column_mapping)

            # Add upload timestamp
            combined_df['Upload_Date'] = datetime.now()

            col1, col2, col3 = st.columns(3)

            with col1:
                if st.button("💾 Save to Session", type="primary", key="save_budtender_session"):
                    st.session_state.budtender_data = combined_df
                    st.success(f"Saved {len(combined_df):,} budtender records to session!")
                    st.rerun()

            with col2:
                if s3_manager.is_configured():
                    if st.button("☁️ Upload to S3", key="upload_budtender_s3"):
                        with st.spinner("Uploading to S3..."):
                            try:
                                # Convert to CSV and upload
                                csv_buffer = io.StringIO()
                                combined_df.to_csv(csv_buffer, index=False)
                                csv_content = csv_buffer.getvalue()

                                s3_manager.s3_client.put_object(
                                    Bucket=s3_manager.bucket_name,
                                    Key="data/budtender_performance.csv",
                                    Body=csv_content.encode('utf-8'),
                                    ContentType='text/csv'
                                )

                                # Also save to session
                                st.session_state.budtender_data = combined_df
                                st.success(f"Uploaded {len(combined_df):,} records to S3!")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Upload failed: {str(e)}")
                else:
                    st.info("Configure S3 to enable cloud storage")

            with col3:
                # Download combined data
                csv = combined_df.to_csv(index=False)
                st.download_button(
                    "📥 Download Combined",
                    csv,
                    "budtender_performance_combined.csv",
                    "text/csv",
                    key="download_budtender"
                )

            # Show combined summary
            st.markdown("---")
            st.subheader("Combined Data Summary")

            summary_col1, summary_col2, summary_col3, summary_col4 = st.columns(4)

            with summary_col1:
                total_employees = combined_df['Employee'].nunique()
                st.metric("Total Budtenders", total_employees)

            with summary_col2:
                total_brands = combined_df['Product_Brand'].nunique() if 'Product_Brand' in combined_df.columns else 0
                st.metric("Unique Brands", total_brands)

            with summary_col3:
                total_units = combined_df['Units_Sold'].sum() if 'Units_Sold' in combined_df.columns else 0
                st.metric("Total Units Sold", f"{total_units:,.0f}")

            with summary_col4:
                total_net = combined_df['Net_Sales'].sum() if 'Net_Sales' in combined_df.columns else 0
                st.metric("Total Net Sales", f"${total_net:,.0f}")

            # Top performers preview
            st.markdown("---")
            st.subheader("Top Performers Preview")

            preview_col1, preview_col2 = st.columns(2)

            with preview_col1:
                st.markdown("**Top 10 Budtenders by Net Sales**")
                if 'Net_Sales' in combined_df.columns:
                    top_budtenders = combined_df.groupby('Employee').agg({
                        'Net_Sales': 'sum',
                        'Units_Sold': 'sum',
                        'Product_Brand': 'nunique'
                    }).round(2)
                    top_budtenders.columns = ['Total Sales', 'Units Sold', 'Brands Sold']
                    top_budtenders = top_budtenders.sort_values('Total Sales', ascending=False).head(10)
                    top_budtenders['Total Sales'] = top_budtenders['Total Sales'].apply(lambda x: f"${x:,.0f}")
                    st.dataframe(top_budtenders, use_container_width=True)

            with preview_col2:
                st.markdown("**Top 10 Brands by Units Sold**")
                if 'Units_Sold' in combined_df.columns and 'Product_Brand' in combined_df.columns:
                    top_brands = combined_df.groupby('Product_Brand').agg({
                        'Units_Sold': 'sum',
                        'Net_Sales': 'sum',
                        'Employee': 'nunique'
                    }).round(2)
                    top_brands.columns = ['Units Sold', 'Net Sales', 'Budtenders']
                    top_brands = top_brands.sort_values('Units Sold', ascending=False).head(10)
                    top_brands['Net Sales'] = top_brands['Net Sales'].apply(lambda x: f"${x:,.0f}")
                    st.dataframe(top_brands, use_container_width=True)

        else:
            st.info("Upload budtender performance files above to get started.")

        # Show existing data if available
        st.markdown("---")
        st.subheader("Current Budtender Data")

        if st.session_state.budtender_data is not None:
            df = st.session_state.budtender_data
            st.success(f"**{len(df):,}** budtender performance records loaded")

            # Quick stats
            stat_col1, stat_col2, stat_col3, stat_col4 = st.columns(4)
            with stat_col1:
                st.metric("Budtenders", df['Employee'].nunique())
            with stat_col2:
                brands_col = 'Product_Brand' if 'Product_Brand' in df.columns else 'Product Brand'
                st.metric("Brands", df[brands_col].nunique() if brands_col in df.columns else 0)
            with stat_col3:
                units_col = 'Units_Sold' if 'Units_Sold' in df.columns else 'Units Sold'
                st.metric("Total Units", f"{df[units_col].sum():,.0f}" if units_col in df.columns else "N/A")
            with stat_col4:
                sales_col = 'Net_Sales' if 'Net_Sales' in df.columns else 'Net Sales'
                st.metric("Total Sales", f"${df[sales_col].sum():,.0f}" if sales_col in df.columns else "N/A")

            with st.expander("View Data", expanded=False):
                st.dataframe(df.head(100), height=400)
        else:
            st.info("No budtender data loaded. Upload files above or reload from S3.")

            # Load from S3 button
            if s3_manager.is_configured():
                if st.button("📥 Load from S3", key="load_budtender_s3"):
                    with st.spinner("Loading from S3..."):
                        try:
                            response = s3_manager.s3_client.get_object(
                                Bucket=s3_manager.bucket_name,
                                Key="data/budtender_performance.csv"
                            )
                            df = pd.read_csv(io.BytesIO(response['Body'].read()))
                            st.session_state.budtender_data = df
                            st.success(f"Loaded {len(df):,} records from S3!")
                            st.rerun()
                        except s3_manager.s3_client.exceptions.NoSuchKey:
                            st.info("No budtender data found in S3. Upload files to get started.")
                        except Exception as e:
                            st.error(f"Error loading from S3: {str(e)}")

    # =========================================================================
    # DEFINE CONTEXT TAB
    # =========================================================================
    with context_tab:
        st.markdown("""
        Provide business context that the insights engine will use when generating recommendations.
        This can include strategic decisions, operational changes, market insights, or any other
        information that would help generate more relevant business insights.
        """)

        st.markdown("---")

        # Context entry form
        st.subheader("Add New Context")

        context_text = st.text_area(
            "Business Context",
            placeholder="Enter business context here. Examples:\n• We will no longer carry [brand] products starting next month\n• Our flagship store is undergoing renovations and will have reduced foot traffic\n• We're focusing on premium products for the next quarter\n• New competitor opened nearby last week",
            height=200,
            key="context_text_input",
            help="Provide any relevant business context that would help the insights engine generate better recommendations."
        )

        author_name = st.text_input(
            "Your Name",
            placeholder="Enter your name",
            key="context_author_input",
            help="Who is providing this context?"
        )

        if st.button("Save Context", type="primary", disabled=not (context_text and author_name)):
            if BUSINESS_CONTEXT_AVAILABLE:
                try:
                    context_service = get_business_context_service()
                    if context_service:
                        result = context_service.add_context(context_text, author_name)
                        st.success(f"✅ Context saved successfully! (ID: {result['context_id'][:8]}...)")
                        st.rerun()
                    else:
                        st.error("❌ Could not connect to context service. Check AWS configuration.")
                except Exception as e:
                    st.error(f"❌ Failed to save context: {str(e)}")
            else:
                st.error("❌ Business context service not available. Check that business_context.py is installed.")

        st.markdown("---")

        # View existing context
        st.subheader("Existing Context Entries")

        if BUSINESS_CONTEXT_AVAILABLE:
            try:
                context_service = get_business_context_service()
                if context_service:
                    contexts = context_service.get_all_context()

                    if contexts:
                        for ctx in contexts:
                            with st.expander(f"📝 {ctx.get('created_date', 'Unknown date')} - {ctx.get('author_name', 'Unknown')}"):
                                st.markdown(f"**Context:**\n{ctx.get('context_text', '')}")
                                st.caption(f"Added: {ctx.get('created_at', 'Unknown')} | ID: {ctx.get('context_id', '')[:8]}...")

                                # Delete button
                                if st.button(f"🗑️ Delete", key=f"delete_ctx_{ctx.get('context_id')}"):
                                    if context_service.delete_context(ctx.get('context_id')):
                                        st.success("Context deleted!")
                                        st.rerun()
                                    else:
                                        st.error("Failed to delete context")
                    else:
                        st.info("No context entries yet. Add your first context above!")
                else:
                    st.warning("Could not connect to context service. Check AWS configuration.")
            except Exception as e:
                st.error(f"Error loading context: {str(e)}")
        else:
            st.warning("Business context service not available.")

    # =========================================================================
    # BRAND MAPPING TAB
    # =========================================================================
    with brand_mapping_tab:
        # Render the brand-product mapping interface
        render_brand_product_mapping(st.session_state, s3_manager)

    # =========================================================================
    # INDUSTRY RESEARCH TAB (if available)
    # =========================================================================
    if research_tab is not None:
        with research_tab:
            if RESEARCH_AVAILABLE:
                render_research_page()
            else:
                st.error("Research integration module not found.")

    # =========================================================================
    # SEO ANALYSIS TAB (if available)
    # =========================================================================
    if seo_tab is not None:
        with seo_tab:
            if SEO_AVAILABLE:
                render_seo_page()
            else:
                st.error("SEO integration module not found.")

    # =========================================================================
    # QR PORTAL TAB (if available)
    # =========================================================================
    if qr_tab is not None:
        with qr_tab:
            if QR_AVAILABLE:
                render_qr_page()
            else:
                st.error("QR Code integration module not found.")

    # Data management section
    st.markdown("---")
    st.subheader("🗂️ Data Management")
    
    mgmt_col1, mgmt_col2, mgmt_col3 = st.columns(3)
    
    with mgmt_col1:
        if st.button("🔄 Reload from S3", type="primary", width='stretch'):
            with st.spinner("Reloading data from S3..."):
                # Clear all caches to ensure fresh data
                _clear_all_data_caches()

                # Reload all data from S3
                loaded_data = s3_manager.load_all_data_from_s3(processor)

                if loaded_data['sales'] is not None:
                    st.session_state.sales_data = loaded_data['sales']
                if loaded_data['brand'] is not None:
                    st.session_state.brand_data = loaded_data['brand']
                if loaded_data['product'] is not None:
                    st.session_state.product_data = loaded_data['product']
                if loaded_data['customer'] is not None:
                    st.session_state.customer_data = loaded_data['customer']
                if loaded_data['invoice'] is not None:
                    st.session_state.invoice_data = loaded_data['invoice']

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
                if st.session_state.customer_data is not None:
                    loaded_items.append(f"Customers ({len(st.session_state.customer_data)})")
                if st.session_state.invoice_data is not None:
                    loaded_items.append(f"Invoices ({len(st.session_state.invoice_data)})")

                if loaded_items:
                    st.success(f"✅ Reloaded: {', '.join(loaded_items)}")
                else:
                    st.info("No data found in S3")
                st.rerun()
    
    with mgmt_col2:
        if st.button("🗑️ Clear Session Data", type="secondary", width='stretch'):
            st.session_state.sales_data = None
            st.session_state.brand_data = None
            st.session_state.product_data = None
            st.session_state.customer_data = None
            st.session_state.invoice_data = None
            st.session_state.budtender_data = None
            st.session_state.data_loaded_from_s3 = False
            st.success("Session data cleared! (S3 data preserved)")
            st.rerun()
    
    with mgmt_col3:
        if st.session_state.sales_data is not None or st.session_state.brand_data is not None:
            if st.button("📥 Export Summary", width='stretch'):
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
