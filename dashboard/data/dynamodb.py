"""
DynamoDB data loading utilities with caching support.
"""

from decimal import Decimal
from typing import Optional
import hashlib
import pandas as pd

# Try to import caching utilities
try:
    from ..core.cache_manager import get_cache_manager, CacheLevel
    CACHE_AVAILABLE = True
except ImportError:
    CACHE_AVAILABLE = False
    get_cache_manager = None
    CacheLevel = None


def get_dynamodb_table_hash(invoice_service) -> str:
    """
    Get a hash of the DynamoDB table state for cache invalidation.

    Args:
        invoice_service: InvoiceDataService instance

    Returns:
        Hash string representing table state
    """
    try:
        response = invoice_service.dynamodb.meta.client.describe_table(
            TableName=invoice_service.line_items_table_name
        )
        item_count = response['Table'].get('ItemCount', 0)
        table_size = response['Table'].get('TableSizeBytes', 0)
        return hashlib.md5(f"{item_count}:{table_size}".encode()).hexdigest()
    except Exception:
        return ""


def load_invoice_data_from_dynamodb(
    invoice_service,
    use_cache: bool = True,
    cache_ttl: int = 3600
) -> Optional[pd.DataFrame]:
    """
    Load invoice line items from DynamoDB and convert to pandas DataFrame.
    Supports caching for improved performance.

    Args:
        invoice_service: InvoiceDataService instance
        use_cache: Whether to use caching (default True)
        cache_ttl: Cache time-to-live in seconds (default 1 hour)

    Returns:
        pd.DataFrame with invoice line items, or None if loading fails
    """
    cache_key = None
    cache_mgr = None

    # Check cache first if enabled
    if use_cache and CACHE_AVAILABLE:
        try:
            cache_mgr = get_cache_manager()
            table_hash = get_dynamodb_table_hash(invoice_service)
            cache_key = f"dynamodb_invoice_data_{table_hash}"

            if cache_mgr:
                cached_df = cache_mgr.get(cache_key)
                if cached_df is not None:
                    return cached_df
        except Exception:
            pass  # Continue without cache

    try:
        # Get the line items table
        line_items_table = invoice_service.dynamodb.Table(
            invoice_service.line_items_table_name
        )

        # Scan all line items
        response = line_items_table.scan()
        items = response.get('Items', [])

        # Handle pagination
        while 'LastEvaluatedKey' in response:
            response = line_items_table.scan(
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response.get('Items', []))

        if not items:
            return None

        # Convert DynamoDB items to DataFrame
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

        # Rename columns to match expected format
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

        for old_name, new_name in column_mapping.items():
            if old_name in df.columns:
                df.rename(columns={old_name: new_name}, inplace=True)

        # Convert date columns
        if 'Invoice Date' in df.columns:
            df['Invoice Date'] = pd.to_datetime(df['Invoice Date'], errors='coerce')
        if 'Download Date' in df.columns:
            df['Download Date'] = pd.to_datetime(df['Download Date'], errors='coerce')

        # Add source column
        df['Data Source'] = 'DynamoDB'

        # Cache the result if caching is enabled
        if use_cache and cache_mgr and cache_key:
            try:
                cache_mgr.set(
                    cache_key,
                    df,
                    ttl_seconds=cache_ttl,
                    levels=[CacheLevel.SESSION]
                )
            except Exception:
                pass  # Don't fail if caching fails

        return df

    except Exception as e:
        print(f"Error loading invoice data from DynamoDB: {e}")
        return None


def get_invoice_summary_cached(
    invoice_service,
    start_date: str = None,
    end_date: str = None,
    use_cache: bool = True
) -> dict:
    """
    Get aggregated invoice summary with caching support.

    Args:
        invoice_service: InvoiceDataService instance
        start_date: Optional start date filter (YYYY-MM-DD)
        end_date: Optional end date filter (YYYY-MM-DD)
        use_cache: Whether to use caching

    Returns:
        Dictionary with invoice summary statistics
    """
    cache_key = None
    cache_mgr = None

    if use_cache and CACHE_AVAILABLE:
        try:
            cache_mgr = get_cache_manager()
            table_hash = get_dynamodb_table_hash(invoice_service)
            cache_key = f"dynamodb_invoice_summary_{table_hash}_{start_date}_{end_date}"

            if cache_mgr:
                cached = cache_mgr.get(cache_key)
                if cached is not None:
                    return cached
        except Exception:
            pass

    # Get summary from service
    summary = invoice_service.get_invoice_summary(start_date, end_date)

    # Cache result
    if use_cache and cache_mgr and cache_key:
        try:
            cache_mgr.set(cache_key, summary, ttl_seconds=3600, levels=[CacheLevel.SESSION])
        except Exception:
            pass

    return summary


def get_product_summary_cached(
    invoice_service,
    start_date: str = None,
    end_date: str = None,
    use_cache: bool = True
) -> dict:
    """
    Get product-level aggregations with caching support.

    Args:
        invoice_service: InvoiceDataService instance
        start_date: Optional start date filter
        end_date: Optional end date filter
        use_cache: Whether to use caching

    Returns:
        Dictionary with product summary statistics
    """
    cache_key = None
    cache_mgr = None

    if use_cache and CACHE_AVAILABLE:
        try:
            cache_mgr = get_cache_manager()
            table_hash = get_dynamodb_table_hash(invoice_service)
            cache_key = f"dynamodb_product_summary_{table_hash}_{start_date}_{end_date}"

            if cache_mgr:
                cached = cache_mgr.get(cache_key)
                if cached is not None:
                    return cached
        except Exception:
            pass

    # Get summary from service
    summary = invoice_service.get_product_summary(start_date, end_date)

    # Cache result
    if use_cache and cache_mgr and cache_key:
        try:
            cache_mgr.set(cache_key, summary, ttl_seconds=3600, levels=[CacheLevel.SESSION])
        except Exception:
            pass

    return summary
