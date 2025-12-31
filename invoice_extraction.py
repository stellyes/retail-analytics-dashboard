"""
Invoice Extraction Module - Direct PDF Parsing
Extracts structured data from Treez invoice PDFs using PDF parsing libraries.
No Claude API calls needed - saves costs and improves speed.

Usage:
    # Single invoice
    python invoice_extraction.py "path/to/invoice.pdf"

    # Batch extract and store in DynamoDB
    python invoice_extraction.py --batch invoices/

    # Extract and save to JSON
    python invoice_extraction.py --batch invoices/ --output invoices.json
"""

import os
import re
import json
from typing import List, Dict, Optional, Tuple
from datetime import datetime
from decimal import Decimal
import PyPDF2

try:
    import boto3
    from boto3.dynamodb.conditions import Key, Attr
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False


class TreezInvoiceParser:
    """
    Parser for Treez-formatted invoice PDFs.
    Extracts data using PDF text extraction and regex patterns.
    """

    def __init__(self):
        self.extraction_errors = []

    def extract_from_pdf(self, pdf_path: str) -> Dict:
        """
        Extract invoice data from a Treez PDF invoice.

        Args:
            pdf_path: Path to the PDF invoice file

        Returns:
            Dictionary containing structured invoice data
        """
        try:
            # Read PDF text
            with open(pdf_path, 'rb') as f:
                pdf_reader = PyPDF2.PdfReader(f)
                text = ""
                for page in pdf_reader.pages:
                    text += page.extract_text() + "\n"

            # Parse the invoice
            invoice_data = self._parse_treez_invoice(text)

            # Add metadata
            invoice_data['source_file'] = os.path.basename(pdf_path)
            invoice_data['extracted_at'] = datetime.now().isoformat()
            invoice_data['extraction_method'] = 'pdf_parsing'

            return invoice_data

        except Exception as e:
            self.extraction_errors.append(f"Error extracting {pdf_path}: {str(e)}")
            return {
                'error': str(e),
                'source_file': os.path.basename(pdf_path),
                'extracted_at': datetime.now().isoformat()
            }

    def _parse_treez_invoice(self, text: str) -> Dict:
        """Parse Treez invoice format from extracted text."""

        # Initialize invoice data
        invoice = {
            'vendor': None,
            'vendor_license': None,
            'vendor_address': None,
            'customer_name': None,
            'customer_address': None,
            'invoice_number': None,
            'invoice_id': None,
            'invoice_date': None,
            'created_by': None,
            'payment_terms': None,
            'status': None,
            'invoice_subtotal': 0.0,
            'invoice_discount': 0.0,
            'invoice_fees': 0.0,
            'invoice_tax': 0.0,
            'invoice_total': 0.0,
            'payments': 0.0,
            'balance': 0.0,
            'currency': 'USD',
            'line_items': []
        }

        lines = text.split('\n')

        # Extract vendor info (top left section)
        vendor_pattern = r'^([A-Z][A-Z\s,\.]+LLC|[A-Z][A-Z\s,\.]+INC)'
        for i, line in enumerate(lines):
            if re.match(vendor_pattern, line.strip()):
                invoice['vendor'] = line.strip()
                # Next lines are likely address and license
                if i + 1 < len(lines):
                    invoice['vendor_address'] = lines[i + 1].strip()
                if i + 2 < len(lines):
                    license_match = re.search(r'C\d{2}-\d{7}', lines[i + 2])
                    if license_match:
                        invoice['vendor_license'] = license_match.group(0)
                break

        # Extract customer info
        customer_pattern = r'(Barbary Coast Dispensary|[\w\s]+Dispensary)'
        for i, line in enumerate(lines):
            if re.search(customer_pattern, line):
                invoice['customer_name'] = re.search(customer_pattern, line).group(1).strip()
                # Next line is likely address
                if i + 1 < len(lines):
                    addr_match = re.search(r'\d+\s+[\w\s]+,\s*[\w\s]+,\s*[A-Z]{2}', lines[i + 1])
                    if addr_match:
                        invoice['customer_address'] = addr_match.group(0).strip()
                break

        # Extract invoice number and ID
        # Try multiple patterns for invoice number
        invoice_num_match = re.search(r'INVOICE#?\s*(\d+)', text)
        if not invoice_num_match:
            # Try alternate pattern with newlines
            invoice_num_match = re.search(r'INVOICE\s*#?\s*(\d+)', text, re.MULTILINE)

        if invoice_num_match:
            invoice['invoice_number'] = invoice_num_match.group(1)
            invoice['invoice_id'] = invoice_num_match.group(1)

        # Extract status
        if 'FULFILLED' in text:
            invoice['status'] = 'FULFILLED'

        # Extract dates - try multiple patterns
        date_pattern = r'Created:\s*(\d{1,2})/(\d{1,2})/(\d{4})'
        date_match = re.search(date_pattern, text)
        if not date_match:
            # Try alternate pattern
            date_pattern = r'Created:\s*(\d{1,2})\s*/\s*(\d{1,2})\s*/\s*(\d{4})'
            date_match = re.search(date_pattern, text, re.MULTILINE)

        if date_match:
            month, day, year = date_match.groups()
            invoice['invoice_date'] = f"{year}-{month.zfill(2)}-{day.zfill(2)}"

        # Extract created by
        created_by_match = re.search(r'Created by:\s*([\w\s]+)', text)
        if created_by_match:
            invoice['created_by'] = created_by_match.group(1).strip()

        # Extract payment terms
        cod_match = re.search(r'COD\s*-\s*\d{1,2}/\d{1,2}/\d{4}', text)
        if cod_match:
            invoice['payment_terms'] = 'COD'

        # Extract line items using table parsing
        invoice['line_items'] = self._extract_line_items(text)

        # Extract totals from bottom of invoice
        subtotal_match = re.search(r'Subtotal\s+\$?([\d,]+\.\d{2})', text)
        if subtotal_match:
            invoice['invoice_subtotal'] = float(subtotal_match.group(1).replace(',', ''))

        discount_match = re.search(r'Discounts\s+\$?([\d,]+\.\d{2})', text)
        if discount_match:
            invoice['invoice_discount'] = float(discount_match.group(1).replace(',', ''))

        fees_match = re.search(r'Fees\s+\$?([\d,]+\.\d{2})', text)
        if fees_match:
            invoice['invoice_fees'] = float(fees_match.group(1).replace(',', ''))

        tax_match = re.search(r'Excise Tax\s+\$?([\d,]+\.\d{2})', text)
        if tax_match:
            invoice['invoice_tax'] = float(tax_match.group(1).replace(',', ''))

        total_match = re.search(r'Total Cost\s+\$?([\d,]+\.\d{2})', text)
        if total_match:
            invoice['invoice_total'] = float(total_match.group(1).replace(',', ''))

        payments_match = re.search(r'Payments\s+\$?([\d,]+\.\d{2})', text)
        if payments_match:
            invoice['payments'] = float(payments_match.group(1).replace(',', ''))

        balance_match = re.search(r'Balance\s+\$?([\d,]+\.\d{2})', text)
        if balance_match:
            invoice['balance'] = float(balance_match.group(1).replace(',', ''))

        return invoice

    def _extract_line_items(self, text: str) -> List[Dict]:
        """Extract line items from invoice text."""
        line_items = []

        # Pattern to match line items in the table
        # Item # | Brand | Product | Type-Subtype | Trace Treez ID | SKU | Units | Cost | Excise/unit | Total Cost | Total Cost w/Excise
        item_pattern = r'(\d+)\s+([A-Z][A-Z\s&]+?)\s+(.+?)\s+(PREROLL|EXTRACT|FLOWER|CARTRIDGE)\s*-\s*(\w+)\s+(1A\w+)\s+(\d+)\s+\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})'

        matches = re.finditer(item_pattern, text, re.MULTILINE)

        for match in matches:
            item_num, brand, product, prod_type, subtype, trace_id, units, cost, excise, total_cost, total_w_excise = match.groups()

            # Clean up brand name
            brand = brand.strip()

            # Determine if promo item
            is_promo = '[PROMO]' in product or 'PROMO' in product

            # Extract product name and details
            product_clean = product.strip()

            # Extract strain/flavor from product name
            strain = None
            strain_match = re.search(r'([A-Z][A-Z\s]+?)(?:\[|$)', product_clean)
            if strain_match:
                strain = strain_match.group(1).strip()

            # Extract size
            size_match = re.search(r'(\d+\.?\d*[GM])', product_clean, re.IGNORECASE)
            unit_size = size_match.group(1) if size_match else None

            line_item = {
                'line_number': int(item_num),
                'brand': brand,
                'product_name': product_clean,
                'product_type': prod_type,
                'product_subtype': subtype,
                'strain': strain,
                'unit_size': unit_size,
                'trace_id': trace_id,
                'sku_units': int(units),
                'unit_cost': float(cost.replace(',', '')),
                'excise_per_unit': float(excise.replace(',', '')),
                'total_cost': float(total_cost.replace(',', '')),
                'total_cost_with_excise': float(total_w_excise.replace(',', '')),
                'is_promo': is_promo
            }

            line_items.append(line_item)

        return line_items


class InvoiceDataService:
    """
    Service for storing and retrieving invoice data from DynamoDB.
    Provides cost-efficient data access for Claude analysis.
    """

    def __init__(self, aws_access_key: str = None, aws_secret_key: str = None, region: str = 'us-west-1'):
        """Initialize DynamoDB client."""
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 required. Install with: pip install boto3")

        # Initialize boto3 client
        session_kwargs = {'region_name': region}
        if aws_access_key and aws_secret_key:
            session_kwargs['aws_access_key_id'] = aws_access_key
            session_kwargs['aws_secret_access_key'] = aws_secret_key

        self.dynamodb = boto3.resource('dynamodb', **session_kwargs)
        self.region = region

        # Table names
        self.invoices_table_name = 'retail-invoices'
        self.line_items_table_name = 'retail-invoice-line-items'
        self.aggregations_table_name = 'retail-invoice-aggregations'

    def create_tables(self):
        """Create DynamoDB tables if they don't exist."""
        # Invoices table
        try:
            invoices_table = self.dynamodb.create_table(
                TableName=self.invoices_table_name,
                KeySchema=[
                    {'AttributeName': 'invoice_id', 'KeyType': 'HASH'},
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'invoice_id', 'AttributeType': 'S'},
                    {'AttributeName': 'invoice_date', 'AttributeType': 'S'},
                    {'AttributeName': 'vendor', 'AttributeType': 'S'},
                ],
                GlobalSecondaryIndexes=[
                    {
                        'IndexName': 'date-index',
                        'KeySchema': [
                            {'AttributeName': 'invoice_date', 'KeyType': 'HASH'},
                        ],
                        'Projection': {'ProjectionType': 'ALL'},
                        'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
                    },
                    {
                        'IndexName': 'vendor-date-index',
                        'KeySchema': [
                            {'AttributeName': 'vendor', 'KeyType': 'HASH'},
                            {'AttributeName': 'invoice_date', 'KeyType': 'RANGE'},
                        ],
                        'Projection': {'ProjectionType': 'ALL'},
                        'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
                    }
                ],
                ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            )
            print(f"Creating {self.invoices_table_name} table...")
            invoices_table.wait_until_exists()
        except self.dynamodb.meta.client.exceptions.ResourceInUseException:
            print(f"Table {self.invoices_table_name} already exists")

        # Line items table
        try:
            line_items_table = self.dynamodb.create_table(
                TableName=self.line_items_table_name,
                KeySchema=[
                    {'AttributeName': 'invoice_id', 'KeyType': 'HASH'},
                    {'AttributeName': 'line_number', 'KeyType': 'RANGE'},
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'invoice_id', 'AttributeType': 'S'},
                    {'AttributeName': 'line_number', 'AttributeType': 'N'},
                    {'AttributeName': 'brand', 'AttributeType': 'S'},
                    {'AttributeName': 'product_type', 'AttributeType': 'S'},
                ],
                GlobalSecondaryIndexes=[
                    {
                        'IndexName': 'brand-index',
                        'KeySchema': [
                            {'AttributeName': 'brand', 'KeyType': 'HASH'},
                        ],
                        'Projection': {'ProjectionType': 'ALL'},
                        'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
                    },
                    {
                        'IndexName': 'product-type-index',
                        'KeySchema': [
                            {'AttributeName': 'product_type', 'KeyType': 'HASH'},
                        ],
                        'Projection': {'ProjectionType': 'ALL'},
                        'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
                    }
                ],
                ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            )
            print(f"Creating {self.line_items_table_name} table...")
            line_items_table.wait_until_exists()
        except self.dynamodb.meta.client.exceptions.ResourceInUseException:
            print(f"Table {self.line_items_table_name} already exists")

        # Aggregations table (for pre-computed summaries)
        try:
            agg_table = self.dynamodb.create_table(
                TableName=self.aggregations_table_name,
                KeySchema=[
                    {'AttributeName': 'agg_type', 'KeyType': 'HASH'},
                    {'AttributeName': 'agg_key', 'KeyType': 'RANGE'},
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'agg_type', 'AttributeType': 'S'},
                    {'AttributeName': 'agg_key', 'AttributeType': 'S'},
                ],
                ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            )
            print(f"Creating {self.aggregations_table_name} table...")
            agg_table.wait_until_exists()
        except self.dynamodb.meta.client.exceptions.ResourceInUseException:
            print(f"Table {self.aggregations_table_name} already exists")

        print("All tables ready!")

    def store_invoice(self, invoice_data: Dict) -> bool:
        """Store invoice and line items in DynamoDB."""
        if 'error' in invoice_data:
            return False

        try:
            invoices_table = self.dynamodb.Table(self.invoices_table_name)
            line_items_table = self.dynamodb.Table(self.line_items_table_name)

            # Prepare invoice header
            invoice_header = {
                'invoice_id': invoice_data['invoice_id'] or invoice_data['invoice_number'],
                'invoice_number': invoice_data['invoice_number'],
                'vendor': invoice_data['vendor'],
                'vendor_license': invoice_data.get('vendor_license'),
                'vendor_address': invoice_data.get('vendor_address'),
                'customer_name': invoice_data.get('customer_name'),
                'customer_address': invoice_data.get('customer_address'),
                'invoice_date': invoice_data.get('invoice_date'),
                'created_by': invoice_data.get('created_by'),
                'payment_terms': invoice_data.get('payment_terms'),
                'status': invoice_data.get('status'),
                'subtotal': Decimal(str(invoice_data.get('invoice_subtotal', 0))),
                'discount': Decimal(str(invoice_data.get('invoice_discount', 0))),
                'fees': Decimal(str(invoice_data.get('invoice_fees', 0))),
                'tax': Decimal(str(invoice_data.get('invoice_tax', 0))),
                'total': Decimal(str(invoice_data.get('invoice_total', 0))),
                'balance': Decimal(str(invoice_data.get('balance', 0))),
                'source_file': invoice_data.get('source_file'),
                'extracted_at': invoice_data.get('extracted_at'),
                'line_item_count': len(invoice_data.get('line_items', []))
            }

            # Store invoice header
            invoices_table.put_item(Item=invoice_header)

            # Store line items
            invoice_id = invoice_header['invoice_id']
            for item in invoice_data.get('line_items', []):
                line_item = {
                    'invoice_id': invoice_id,
                    'line_number': item['line_number'],
                    'brand': item['brand'],
                    'product_name': item['product_name'],
                    'product_type': item['product_type'],
                    'product_subtype': item['product_subtype'],
                    'strain': item.get('strain'),
                    'unit_size': item.get('unit_size'),
                    'trace_id': item['trace_id'],
                    'sku_units': item['sku_units'],
                    'unit_cost': Decimal(str(item['unit_cost'])),
                    'excise_per_unit': Decimal(str(item['excise_per_unit'])),
                    'total_cost': Decimal(str(item['total_cost'])),
                    'total_cost_with_excise': Decimal(str(item['total_cost_with_excise'])),
                    'is_promo': item.get('is_promo', False),
                    'invoice_date': invoice_data.get('invoice_date')
                }
                line_items_table.put_item(Item=line_item)

            return True

        except Exception as e:
            print(f"Error storing invoice: {e}")
            return False

    def get_invoice_summary(self, start_date: str = None, end_date: str = None) -> Dict:
        """
        Get aggregated invoice summary for Claude analysis.
        Returns only the essential data needed for analysis.
        """
        invoices_table = self.dynamodb.Table(self.invoices_table_name)

        # Query invoices
        if start_date and end_date:
            response = invoices_table.query(
                IndexName='date-index',
                KeyConditionExpression=Key('invoice_date').between(start_date, end_date)
            )
        else:
            response = invoices_table.scan()

        invoices = response.get('Items', [])

        # Calculate summary statistics
        total_invoices = len(invoices)
        total_value = sum(float(inv.get('total', 0)) for inv in invoices)
        total_items = sum(int(inv.get('line_item_count', 0)) for inv in invoices)

        vendors = {}
        for inv in invoices:
            vendor = inv.get('vendor')
            if vendor:
                if vendor not in vendors:
                    vendors[vendor] = {'count': 0, 'total': 0}
                vendors[vendor]['count'] += 1
                vendors[vendor]['total'] += float(inv.get('total', 0))

        return {
            'total_invoices': total_invoices,
            'total_value': total_value,
            'avg_invoice_value': total_value / total_invoices if total_invoices > 0 else 0,
            'total_line_items': total_items,
            'vendors': vendors,
            'date_range': {
                'start': start_date,
                'end': end_date
            }
        }

    def get_product_summary(self, start_date: str = None, end_date: str = None) -> Dict:
        """Get product-level aggregations for Claude analysis."""
        line_items_table = self.dynamodb.Table(self.line_items_table_name)

        response = line_items_table.scan()
        items = response.get('Items', [])

        # Filter by date if specified
        if start_date or end_date:
            filtered = []
            for item in items:
                item_date = item.get('invoice_date')
                if item_date:
                    if start_date and item_date < start_date:
                        continue
                    if end_date and item_date > end_date:
                        continue
                filtered.append(item)
            items = filtered

        # Aggregate by brand
        brands = {}
        for item in items:
            brand = item.get('brand')
            if brand:
                if brand not in brands:
                    brands[brand] = {
                        'total_units': 0,
                        'total_cost': 0,
                        'product_count': 0
                    }
                brands[brand]['total_units'] += int(item.get('sku_units', 0))
                brands[brand]['total_cost'] += float(item.get('total_cost', 0))
                brands[brand]['product_count'] += 1

        # Aggregate by product type
        product_types = {}
        for item in items:
            ptype = item.get('product_type')
            if ptype:
                if ptype not in product_types:
                    product_types[ptype] = {
                        'total_units': 0,
                        'total_cost': 0,
                        'product_count': 0
                    }
                product_types[ptype]['total_units'] += int(item.get('sku_units', 0))
                product_types[ptype]['total_cost'] += float(item.get('total_cost', 0))
                product_types[ptype]['product_count'] += 1

        return {
            'brands': brands,
            'product_types': product_types,
            'total_items': len(items)
        }


# =============================================================================
# CLI FUNCTIONS
# =============================================================================

def extract_single_invoice(pdf_path: str, store_dynamodb: bool = False,
                          aws_config: Dict = None) -> Dict:
    """Extract a single invoice and optionally store in DynamoDB."""
    print(f"Extracting invoice: {pdf_path}")

    parser = TreezInvoiceParser()
    invoice_data = parser.extract_from_pdf(pdf_path)

    if 'error' in invoice_data:
        print(f"  Error: {invoice_data['error']}")
        return invoice_data

    # Display summary
    print(f"  Invoice #: {invoice_data.get('invoice_number')}")
    print(f"  Vendor: {invoice_data.get('vendor')}")
    print(f"  Date: {invoice_data.get('invoice_date')}")
    print(f"  Total: ${invoice_data.get('invoice_total', 0):,.2f}")
    print(f"  Line Items: {len(invoice_data.get('line_items', []))}")

    # Store in DynamoDB if requested
    if store_dynamodb and aws_config:
        service = InvoiceDataService(**aws_config)
        if service.store_invoice(invoice_data):
            print("  Stored in DynamoDB")

    return invoice_data


def batch_extract_invoices(directory_path: str, store_dynamodb: bool = False,
                          aws_config: Dict = None, output_file: str = None):
    """Extract all invoices in a directory."""
    parser = TreezInvoiceParser()

    pdf_files = [f for f in os.listdir(directory_path) if f.lower().endswith('.pdf')]
    print(f"Found {len(pdf_files)} PDF files\n")

    all_invoices = []
    success_count = 0

    # Initialize DynamoDB service if needed
    service = None
    if store_dynamodb and aws_config:
        service = InvoiceDataService(**aws_config)
        print("Ensuring DynamoDB tables exist...")
        service.create_tables()
        print()

    for i, filename in enumerate(pdf_files, 1):
        pdf_path = os.path.join(directory_path, filename)
        print(f"[{i}/{len(pdf_files)}] {filename}")

        invoice_data = parser.extract_from_pdf(pdf_path)

        if 'error' not in invoice_data:
            all_invoices.append(invoice_data)
            success_count += 1

            print(f"  Invoice #: {invoice_data.get('invoice_number')}")
            print(f"  Total: ${invoice_data.get('invoice_total', 0):,.2f}")
            print(f"  Items: {len(invoice_data.get('line_items', []))}")

            # Store in DynamoDB
            if service:
                if service.store_invoice(invoice_data):
                    print("  Stored in DynamoDB")
        else:
            print(f"  Error: {invoice_data.get('error')}")

        print()

    print(f"\n{'='*60}")
    print(f"Successfully extracted {success_count}/{len(pdf_files)} invoices")

    # Save to JSON if requested
    if output_file and all_invoices:
        with open(output_file, 'w') as f:
            json.dump(all_invoices, f, indent=2, default=str)
        print(f"Saved to {output_file}")

    return all_invoices


if __name__ == "__main__":
    import sys
    import argparse

    parser = argparse.ArgumentParser(description='Extract data from Treez invoice PDFs')
    parser.add_argument('path', nargs='?', help='Path to PDF file or directory')
    parser.add_argument('--batch', action='store_true', help='Batch process directory')
    parser.add_argument('--dynamodb', action='store_true', help='Store in DynamoDB')
    parser.add_argument('--output', help='Output JSON file path')
    parser.add_argument('--aws-key', help='AWS access key ID')
    parser.add_argument('--aws-secret', help='AWS secret access key')
    parser.add_argument('--aws-region', default='us-west-1', help='AWS region')

    args = parser.parse_args()

    if not args.path:
        parser.print_help()
        sys.exit(1)

    # Prepare AWS config
    aws_config = None
    if args.dynamodb:
        aws_config = {
            'aws_access_key': args.aws_key,
            'aws_secret_key': args.aws_secret,
            'region': args.aws_region
        }

    if args.batch or os.path.isdir(args.path):
        batch_extract_invoices(args.path, args.dynamodb, aws_config, args.output)
    else:
        invoice = extract_single_invoice(args.path, args.dynamodb, aws_config)

        # Save to JSON
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(invoice, f, indent=2, default=str)
            print(f"\nSaved to {args.output}")
