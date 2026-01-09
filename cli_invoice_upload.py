#!/usr/bin/env python3
"""
CLI Invoice Upload Script
Uploads invoice PDFs to DynamoDB in bulk, much faster than browser interface.
"""

import os
import sys
from pathlib import Path
from datetime import datetime

# Unbuffered output for real-time progress
os.environ['PYTHONUNBUFFERED'] = '1'

# Add the dashboard package to path
sys.path.insert(0, str(Path(__file__).parent))

from dashboard.services.invoice_extraction import TreezInvoiceParser, InvoiceDataService


def upload_invoices(invoice_dir: str, batch_size: int = 100):
    """Upload all PDF invoices from a directory to DynamoDB."""

    # Initialize services
    print("Initializing services...")
    parser = TreezInvoiceParser()
    invoice_service = InvoiceDataService()

    # Get all PDF files
    pdf_files = list(Path(invoice_dir).glob("*.pdf"))
    total_files = len(pdf_files)
    print(f"Found {total_files} PDF files to process")

    if total_files == 0:
        print("No PDF files found!")
        return

    # Load existing invoices for duplicate checking
    print("Loading existing invoices for duplicate check...")
    existing_invoices = {}
    try:
        invoices_table = invoice_service.dynamodb.Table(invoice_service.invoices_table_name)
        response = invoices_table.scan(
            ProjectionExpression='invoice_number, invoice_id, receiver, customer_name'
        )
        items = response.get('Items', [])
        while 'LastEvaluatedKey' in response:
            response = invoices_table.scan(
                ProjectionExpression='invoice_number, invoice_id, receiver, customer_name',
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response.get('Items', []))

        for item in items:
            inv_num = item.get('invoice_number')
            if inv_num:
                store_name = item.get('receiver') or item.get('customer_name') or ''
                if 'barbary' in store_name.lower():
                    store_key = 'Barbary Coast'
                elif 'grass' in store_name.lower():
                    store_key = 'Grass Roots'
                else:
                    store_key = store_name

                if inv_num not in existing_invoices:
                    existing_invoices[inv_num] = []
                existing_invoices[inv_num].append({'store_key': store_key, 'invoice_id': item.get('invoice_id')})

        print(f"Found {len(existing_invoices)} existing unique invoice numbers")
    except Exception as e:
        print(f"Warning: Could not load existing invoices: {e}")

    # Process files
    results = {'successful': 0, 'duplicates': 0, 'failed': 0}
    start_time = datetime.now()

    for idx, pdf_path in enumerate(pdf_files):
        filename = pdf_path.name

        # Progress update every 10 files
        if idx % 10 == 0:
            elapsed = (datetime.now() - start_time).total_seconds()
            rate = idx / elapsed if elapsed > 0 else 0
            remaining = (total_files - idx) / rate if rate > 0 else 0
            print(f"Processing {idx + 1}/{total_files} ({rate:.1f}/sec, ~{remaining/60:.1f} min remaining): {filename}")

        try:
            # Parse invoice using extract_from_pdf (takes file path)
            invoice_data = parser.extract_from_pdf(str(pdf_path))

            if 'error' in invoice_data:
                results['failed'] += 1
                print(f"  FAILED (parse): {filename} - {invoice_data['error']}")
                continue

            # Add source file info
            invoice_data['source_file'] = filename

            # Check for duplicate
            invoice_number = invoice_data.get('invoice_number')
            store_name = invoice_data.get('receiver') or invoice_data.get('customer_name') or ''

            if 'barbary' in store_name.lower():
                store_key = 'Barbary Coast'
            elif 'grass' in store_name.lower():
                store_key = 'Grass Roots'
            else:
                store_key = store_name

            is_duplicate = False
            if invoice_number and invoice_number in existing_invoices:
                for existing in existing_invoices[invoice_number]:
                    if existing['store_key'] == store_key:
                        is_duplicate = True
                        break

            if is_duplicate:
                results['duplicates'] += 1
                continue

            # Store in DynamoDB
            success = invoice_service.store_invoice(invoice_data)

            if success:
                results['successful'] += 1
                # Add to existing for future duplicate checks in this batch
                if invoice_number:
                    if invoice_number not in existing_invoices:
                        existing_invoices[invoice_number] = []
                    existing_invoices[invoice_number].append({'store_key': store_key, 'invoice_id': invoice_data.get('invoice_id')})
            else:
                results['failed'] += 1
                print(f"  FAILED (store): {filename}")

        except Exception as e:
            results['failed'] += 1
            print(f"  FAILED (exception): {filename} - {str(e)}")

    # Summary
    elapsed = (datetime.now() - start_time).total_seconds()
    print("\n" + "="*60)
    print("UPLOAD COMPLETE")
    print("="*60)
    print(f"Total files: {total_files}")
    print(f"Successful: {results['successful']}")
    print(f"Duplicates skipped: {results['duplicates']}")
    print(f"Failed: {results['failed']}")
    print(f"Time: {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
    print(f"Rate: {total_files/elapsed:.1f} invoices/second")


if __name__ == "__main__":
    # Default to invoice-crawler/invoices directory
    invoice_dir = sys.argv[1] if len(sys.argv) > 1 else "/Users/dan/Desktop/Ryan England Workspace/retail-analytics-dashboard/invoice-crawler/invoices"
    upload_invoices(invoice_dir)
