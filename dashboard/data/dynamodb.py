"""
DynamoDB data loading utilities.
"""

from decimal import Decimal
from typing import Optional
import pandas as pd


def load_invoice_data_from_dynamodb(invoice_service) -> Optional[pd.DataFrame]:
    """
    Load invoice line items from DynamoDB and convert to pandas DataFrame.

    Args:
        invoice_service: InvoiceDataService instance

    Returns:
        pd.DataFrame with invoice line items, or None if loading fails
    """
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

        return df

    except Exception as e:
        print(f"Error loading invoice data from DynamoDB: {e}")
        return None
