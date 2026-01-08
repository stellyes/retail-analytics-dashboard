"""
S3 Data Manager for persistent storage and retrieval.
"""

import io
import re
import json
import hashlib
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
import boto3
from botocore.exceptions import ClientError


class S3DataManager:
    """Manages data persistence with AWS S3."""

    def __init__(
        self,
        bucket_name: str = None,
        aws_access_key: str = None,
        aws_secret_key: str = None,
        aws_region: str = "us-west-2"
    ):
        self.bucket_name = bucket_name
        self.s3_client = None
        self.connection_error = None
        self._initialize_client(aws_access_key, aws_secret_key, aws_region)

    def _initialize_client(
        self,
        aws_access_key: str = None,
        aws_secret_key: str = None,
        aws_region: str = "us-west-2"
    ) -> None:
        """Initialize S3 client with credentials."""
        try:
            import os

            # Try environment variables first
            access_key = aws_access_key or os.environ.get("AWS_ACCESS_KEY_ID")
            secret_key = aws_secret_key or os.environ.get("AWS_SECRET_ACCESS_KEY")
            region = aws_region or os.environ.get("AWS_DEFAULT_REGION", "us-west-2")
            bucket = self.bucket_name or os.environ.get("S3_BUCKET_NAME")

            # Fall back to Streamlit secrets
            if not access_key:
                try:
                    import streamlit as st
                    aws_secrets = st.secrets.get("aws", {})
                    access_key = aws_secrets.get("access_key_id")
                    secret_key = aws_secrets.get("secret_access_key")
                    region = aws_secrets.get("region", "us-west-2")
                    bucket = aws_secrets.get("bucket_name")
                except Exception:
                    pass

            if not bucket:
                self.connection_error = "No bucket_name configured"
                return

            self.bucket_name = bucket

            if access_key and secret_key:
                self.s3_client = boto3.client(
                    's3',
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_key,
                    region_name=region
                )
            else:
                self.connection_error = "Missing AWS credentials"
                return

        except Exception as e:
            self.connection_error = f"S3 initialization error: {e}"
            self.s3_client = None

    def is_configured(self) -> bool:
        """Check if S3 is properly configured."""
        return self.s3_client is not None and self.bucket_name is not None

    def test_connection(self) -> Tuple[bool, str]:
        """Test S3 connection by attempting to list bucket contents."""
        if not self.is_configured():
            return False, self.connection_error or "S3 not configured"

        try:
            self.s3_client.list_objects_v2(Bucket=self.bucket_name, MaxKeys=1)
            return True, f"Connected to bucket: {self.bucket_name}"
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            error_msg = e.response.get('Error', {}).get('Message', str(e))
            return False, f"S3 Error ({error_code}): {error_msg}"
        except Exception as e:
            return False, f"Connection test failed: {e}"

    def upload_file(self, file_obj, s3_key: str) -> Tuple[bool, str]:
        """Upload a file to S3."""
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

    def download_file(self, s3_key: str) -> Optional[pd.DataFrame]:
        """Download a CSV file from S3 and return as DataFrame."""
        if not self.is_configured():
            return None
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            return pd.read_csv(io.BytesIO(response['Body'].read()), low_memory=False)
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                return None
            return None

    def list_files(self, prefix: str = "") -> List[str]:
        """List files in S3 bucket with given prefix."""
        if not self.is_configured():
            return []
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
            return [obj['Key'] for obj in response.get('Contents', [])]
        except ClientError:
            return []

    def save_processed_data(
        self,
        df: pd.DataFrame,
        data_type: str,
        store: str = "combined"
    ) -> Tuple[bool, str]:
        """Save processed data to S3."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        s3_key = f"processed/{store}/{data_type}_{timestamp}.csv"

        buffer = io.BytesIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)

        return self.upload_file(buffer, s3_key)

    def save_brand_product_mapping(self, mapping: dict) -> Tuple[bool, str]:
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
                return {}
            return {}
        except Exception:
            return {}

    def get_data_hash(self) -> str:
        """
        Get a hash representing the current state of all data files in S3.
        Uses S3 ETags (MD5 hashes) combined with file list to detect changes.
        """
        if not self.is_configured():
            return ""

        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix="raw-uploads/"
            )

            if 'Contents' not in response:
                return "empty"

            hash_parts = []
            for obj in sorted(response['Contents'], key=lambda x: x['Key']):
                hash_parts.append(
                    f"{obj['Key']}:{obj['ETag']}:{obj['LastModified'].isoformat()}"
                )

            # Handle pagination
            while response.get('IsTruncated', False):
                response = self.s3_client.list_objects_v2(
                    Bucket=self.bucket_name,
                    Prefix="raw-uploads/",
                    ContinuationToken=response['NextContinuationToken']
                )
                for obj in sorted(response.get('Contents', []), key=lambda x: x['Key']):
                    hash_parts.append(
                        f"{obj['Key']}:{obj['ETag']}:{obj['LastModified'].isoformat()}"
                    )

            # Include mapping file hash
            try:
                mapping_response = self.s3_client.head_object(
                    Bucket=self.bucket_name,
                    Key="config/brand_product_mapping.json"
                )
                hash_parts.append(f"mapping:{mapping_response['ETag']}")
            except ClientError:
                hash_parts.append("mapping:none")

            combined = "|".join(hash_parts)
            return hashlib.md5(combined.encode()).hexdigest()

        except Exception as e:
            print(f"Error computing data hash: {e}")
            return ""

    def load_all_data_from_s3(self, processor) -> Dict[str, Optional[pd.DataFrame]]:
        """
        Load all uploaded data from S3 and return merged DataFrames.

        Args:
            processor: DataProcessor instance for cleaning data

        Returns:
            Dict with 'sales', 'brand', 'product', 'customer', 'invoice' DataFrames
        """
        if not self.is_configured():
            return {
                'sales': None, 'brand': None, 'product': None,
                'customer': None, 'invoice': None
            }

        result = {
            'sales': None, 'brand': None, 'product': None,
            'customer': None, 'invoice': None
        }

        try:
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
                    customer_id_col = (
                        'Customer ID' if 'Customer ID' in result['customer'].columns
                        else 'id'
                    )
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
                    if ('Upload_Start_Date' in result['invoice'].columns and
                            'Invoice Number' in result['invoice'].columns):
                        result['invoice'] = result['invoice'].drop_duplicates(
                            subset=['Invoice Number', 'Upload_Store', 'Upload_Start_Date'],
                            keep='last'
                        )

        except Exception as e:
            print(f"Error loading data from S3: {e}")

        return result

    def _extract_store_from_path(self, path: str) -> str:
        """Extract store ID from S3 file path."""
        parts = path.split('/')
        if len(parts) >= 2:
            return parts[1]
        return 'combined'

    def _extract_date_range_from_path(self, path: str) -> Optional[Tuple[datetime, datetime]]:
        """Extract date range from S3 file path."""
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
