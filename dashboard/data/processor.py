"""
Data processing and cleaning utilities.
"""

from datetime import datetime
import pandas as pd


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
        numeric_cols = [
            'Tickets Count', 'Units Sold', 'Customers Count', 'New Customers',
            'Gross Sales', 'Discounts', 'Returns', 'Net Sales', 'Taxes',
            'Gross Receipts', 'COGS (with excise)', 'Gross Income',
            'Gross Margin %', 'Discount %', 'Cost %',
            'Avg Basket Size', 'Avg Order Value', 'Avg Order Profit'
        ]

        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        return df.sort_values('Date')

    @staticmethod
    def clean_brand_data(df: pd.DataFrame) -> pd.DataFrame:
        """Clean and process Net Sales by Brand data."""
        df = df.copy()

        # Handle column name change: Treez renamed 'Brand' to 'Product Brand'
        if 'Product Brand' in df.columns and 'Brand' not in df.columns:
            df = df.rename(columns={'Product Brand': 'Brand'})

        # Filter out sample records ([DS] = Display Samples, [SS] = Staff Samples)
        original_count = len(df)
        df = df[~df['Brand'].str.startswith(('[DS]', '[SS]'), na=False)]
        filtered_count = original_count - len(df)

        if filtered_count > 0:
            print(f"Filtered out {filtered_count} sample records ([DS]/[SS])")

        # Clean brand name
        df['Brand_Clean'] = df['Brand'].str.strip()

        # Ensure numeric columns
        numeric_cols = ['% of Total Net Sales', 'Gross Margin %', 'Avg Cost (w/o excise)', 'Net Sales']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Filter out rows with zero or negative net sales
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
        date_cols = [
            'Date of Birth', 'Customer Drivers License Expiration Date',
            'Sign-Up Date', 'Last Visit Date', 'First Purchase Date',
            'Customer Medical Id Expiration Date'
        ]

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
        numeric_cols = [
            'Lifetime In-Store Visits', 'Lifetime Transactions',
            'Lifetime Net Sales', 'Lifetime Gross Receipts',
            'Lifetime Discounts', 'Lifetime Avg Order Value',
            'Rewards Points Balance', 'Reward Points ($) Balance'
        ]

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
        numeric_cols = [
            'Total Amount', 'Subtotal', 'Tax', 'Shipping', 'Discount',
            'Quantity', 'Unit Price', 'Line Total'
        ]
        for col in numeric_cols:
            if col in df.columns:
                # Remove currency symbols and commas
                if df[col].dtype == 'object':
                    df[col] = df[col].str.replace('$', '').str.replace(',', '')
                df[col] = pd.to_numeric(df[col], errors='coerce')

        return df
