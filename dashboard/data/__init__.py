"""
Data management module for the Retail Analytics Dashboard.
Provides S3 integration, data processing, and analytics.
"""

from .s3_manager import S3DataManager
from .processor import DataProcessor
from .analytics import AnalyticsEngine
from .dynamodb import (
    load_invoice_data_from_dynamodb,
    get_dynamodb_table_hash,
    get_invoice_summary_cached,
    get_product_summary_cached,
)

__all__ = [
    'S3DataManager',
    'DataProcessor',
    'AnalyticsEngine',
    'load_invoice_data_from_dynamodb',
    'get_dynamodb_table_hash',
    'get_invoice_summary_cached',
    'get_product_summary_cached',
]
