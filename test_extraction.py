#!/usr/bin/env python3
"""
Test script to analyze invoice extraction quality across multiple PDFs.
Identifies patterns of extraction failures for debugging and improvement.
"""

import os
import sys
import json
from collections import defaultdict
from invoice_extraction import TreezInvoiceParser

def test_extraction(invoice_dir: str, sample_size: int = 50):
    """Test extraction on a sample of invoices and report issues."""

    parser = TreezInvoiceParser()

    # Get all PDF files
    pdf_files = sorted([f for f in os.listdir(invoice_dir) if f.lower().endswith('.pdf')])
    print(f"Found {len(pdf_files)} PDF files")

    # Sample evenly across the collection
    step = max(1, len(pdf_files) // sample_size)
    sample_files = pdf_files[::step][:sample_size]
    print(f"Testing {len(sample_files)} sample files\n")

    # Track results
    results = {
        'successful': [],
        'failed': [],
        'issues': defaultdict(list)
    }

    for i, filename in enumerate(sample_files, 1):
        pdf_path = os.path.join(invoice_dir, filename)
        print(f"[{i}/{len(sample_files)}] {filename}")

        try:
            invoice = parser.extract_from_pdf(pdf_path)

            if 'error' in invoice:
                results['failed'].append({
                    'filename': filename,
                    'error': invoice['error']
                })
                print(f"  ERROR: {invoice['error']}")
                continue

            # Check for issues
            issues = []

            # Check vendor
            if not invoice.get('vendor') or invoice.get('vendor') == 'UNKNOWN':
                issues.append('missing_vendor')

            # Check invoice number
            if not invoice.get('invoice_number') or invoice.get('invoice_number') == 'UNKNOWN':
                issues.append('missing_invoice_number')

            # Check date
            if not invoice.get('invoice_date'):
                if invoice.get('_date_extraction_failed'):
                    issues.append('date_null_bytes')
                else:
                    issues.append('missing_date')

            # Check total
            if invoice.get('invoice_total', 0) == 0:
                issues.append('missing_total')

            # Check line items
            line_items = invoice.get('line_items', [])
            if not line_items:
                issues.append('no_line_items')
            else:
                # Check line item quality
                for item in line_items:
                    if item.get('brand') == 'UNKNOWN':
                        if 'unknown_brand' not in issues:
                            issues.append('unknown_brand')
                    if item.get('product_type') == 'UNKNOWN':
                        if 'unknown_product_type' not in issues:
                            issues.append('unknown_product_type')

            # Record result
            result = {
                'filename': filename,
                'invoice_number': invoice.get('invoice_number'),
                'vendor': invoice.get('vendor'),
                'date': invoice.get('invoice_date'),
                'total': invoice.get('invoice_total', 0),
                'line_items': len(line_items),
                'issues': issues
            }

            if issues:
                results['issues']['all'].append(result)
                for issue in issues:
                    results['issues'][issue].append(result)
                print(f"  ISSUES: {', '.join(issues)}")
            else:
                results['successful'].append(result)
                print(f"  OK: Invoice #{invoice.get('invoice_number')}, {len(line_items)} items, ${invoice.get('invoice_total', 0):,.2f}")

        except Exception as e:
            results['failed'].append({
                'filename': filename,
                'error': str(e)
            })
            print(f"  EXCEPTION: {e}")

    # Print summary
    print("\n" + "="*60)
    print("EXTRACTION TEST SUMMARY")
    print("="*60)

    total = len(sample_files)
    successful = len(results['successful'])
    with_issues = len(results['issues']['all'])
    failed = len(results['failed'])

    print(f"\nTotal tested: {total}")
    print(f"Successful: {successful} ({100*successful/total:.1f}%)")
    print(f"With issues: {with_issues} ({100*with_issues/total:.1f}%)")
    print(f"Failed: {failed} ({100*failed/total:.1f}%)")

    print("\n" + "-"*40)
    print("ISSUE BREAKDOWN:")
    print("-"*40)

    issue_types = [k for k in results['issues'].keys() if k != 'all']
    for issue_type in sorted(issue_types, key=lambda x: len(results['issues'][x]), reverse=True):
        count = len(results['issues'][issue_type])
        print(f"  {issue_type}: {count} ({100*count/total:.1f}%)")
        # Show sample filenames
        for item in results['issues'][issue_type][:3]:
            print(f"    - {item['filename']}")

    return results


def analyze_specific_invoice(invoice_dir: str, filename: str):
    """Analyze a specific invoice in detail for debugging."""
    import pdfplumber

    pdf_path = os.path.join(invoice_dir, filename)
    print(f"Analyzing: {pdf_path}\n")

    # Raw text extraction
    print("="*60)
    print("RAW TEXT EXTRACTION")
    print("="*60)

    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages, 1):
            text = page.extract_text()
            print(f"\n--- Page {page_num} ---")
            print(text[:2000] if text else "(no text)")

            # Show tables
            tables = page.extract_tables()
            if tables:
                print(f"\n--- Tables on Page {page_num} ---")
                for t_idx, table in enumerate(tables):
                    print(f"Table {t_idx + 1}: {len(table)} rows")
                    for row in table[:5]:  # First 5 rows
                        print(f"  {row}")
                    if len(table) > 5:
                        print(f"  ... ({len(table) - 5} more rows)")

    # Parsed extraction
    print("\n" + "="*60)
    print("PARSED EXTRACTION")
    print("="*60)

    parser = TreezInvoiceParser()
    invoice = parser.extract_from_pdf(pdf_path)

    print(json.dumps(invoice, indent=2, default=str))


if __name__ == "__main__":
    invoice_dir = "/Users/slimreaper/Documents/invoice-crawler/invoices"

    if len(sys.argv) > 1:
        # Analyze specific file
        analyze_specific_invoice(invoice_dir, sys.argv[1])
    else:
        # Run general test
        results = test_extraction(invoice_dir, sample_size=100)
