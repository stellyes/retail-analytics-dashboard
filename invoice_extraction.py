"""
Invoice Extraction Module
Extracts structured data from vendor invoice PDFs using Claude's vision capabilities.
Works with both text-based and image-based PDFs.

Usage:
    # Single invoice
    python invoice_extraction.py "path/to/invoice.pdf"

    # Batch extract all invoices
    from invoice_extraction import InvoiceExtractor
    extractor = InvoiceExtractor(api_key="your-key")
    df = extractor.extract_from_directory("invoices/")
    df.to_csv("invoices_extracted.csv", index=False)

The extracted CSV can be uploaded to the dashboard via the Data Upload page.
"""

import os
import json
import base64
from typing import List, Dict, Optional
from datetime import datetime
import pandas as pd

try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False


class InvoiceExtractor:
    """
    Extract invoice data using Claude's vision capabilities.
    Works with both text-based and image-based PDFs.
    """

    def __init__(self, api_key: Optional[str] = None):
        """Initialize Claude client."""
        if not ANTHROPIC_AVAILABLE:
            raise ImportError("anthropic package required. Install with: pip install anthropic")

        key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        if not key:
            raise ValueError("ANTHROPIC_API_KEY not found in environment")

        self.client = anthropic.Anthropic(api_key=key)
        self.model = "claude-3-5-haiku-20241022"  # Haiku 3.5 - fast and cost-effective
        self.extraction_errors = []

    def extract_from_pdf(self, pdf_path: str) -> Dict:
        """
        Extract invoice data from a PDF using Claude's vision.

        Args:
            pdf_path: Path to the PDF invoice file

        Returns:
            Dictionary containing structured invoice data
        """
        try:
            # Read PDF and convert to base64
            with open(pdf_path, 'rb') as f:
                pdf_data = base64.standard_b64encode(f.read()).decode('utf-8')

            # Create the prompt for structured extraction
            prompt = self._create_extraction_prompt()

            # Call Claude with the PDF
            message = self.client.messages.create(
                model=self.model,
                max_tokens=4000,
                messages=[{
                    "role": "user",
                    "content": [
                        {
                            "type": "document",
                            "source": {
                                "type": "base64",
                                "media_type": "application/pdf",
                                "data": pdf_data
                            }
                        },
                        {
                            "type": "text",
                            "text": prompt
                        }
                    ]
                }]
            )

            # Parse the response
            response_text = message.content[0].text

            # Extract JSON from response
            invoice_data = self._parse_claude_response(response_text)

            # Add metadata
            invoice_data['source_file'] = os.path.basename(pdf_path)
            invoice_data['extracted_at'] = datetime.now().isoformat()
            invoice_data['extraction_method'] = 'claude_vision'

            return invoice_data

        except Exception as e:
            self.extraction_errors.append(f"Error extracting {pdf_path}: {str(e)}")
            return {
                'error': str(e),
                'source_file': os.path.basename(pdf_path)
            }

    def _create_extraction_prompt(self) -> str:
        """Create the prompt for Claude to extract invoice data."""
        return """You are an expert at extracting structured data from cannabis dispensary vendor invoices.

Please analyze this invoice PDF and extract ALL the data in the following JSON format. Be thorough and accurate.

CRITICAL: Return ONLY valid JSON, no markdown formatting, no explanation text. Start with { and end with }.

{
  "vendor": "Vendor company name",
  "vendor_license": "Vendor cannabis license number (if present)",
  "invoice_number": "Invoice or order number",
  "invoice_date": "YYYY-MM-DD format",
  "customer_name": "Customer/buyer name",
  "customer_license": "Customer cannabis license",
  "customer_address": "Full address",
  "customer_contact": "Contact name",
  "customer_phone": "Phone number",
  "delivery_date": "YYYY-MM-DD or null",
  "payment_due_date": "YYYY-MM-DD or null",
  "payment_terms": "e.g., NET30, Cash on delivery, etc.",
  "sales_rep": "Sales representative name",
  "invoice_subtotal": 0.00,
  "invoice_discount": 0.00,
  "invoice_delivery_fee": 0.00,
  "invoice_tax": 0.00,
  "invoice_total": 0.00,
  "currency": "USD",
  "line_items": [
    {
      "line_number": 1,
      "product_code": "Product SKU or code",
      "item_name": "Full product description",
      "brand": "Brand name",
      "strain": "Strain name or null",
      "product_type": "e.g., Gummies, Concentrate, Flower, Vape, etc.",
      "thc_content": "THC info if present",
      "cbd_content": "CBD info if present",
      "unit_size": "e.g., 1g, 100mg, 10ct",
      "quantity": 0,
      "unit_of_measure": "Each, Package, Case, etc.",
      "list_price": 0.00,
      "discount_percentage": 0,
      "discount_amount": 0.00,
      "unit_price": 0.00,
      "total_price": 0.00,
      "license_number": "Track-and-trace license # if present",
      "batch_number": "Batch or lot number if present"
    }
  ],
  "payment_info": {
    "bank_name": "Bank name if present",
    "account_number": "Account # if present",
    "routing_number": "Routing # if present"
  },
  "notes": "Any special notes, terms, or conditions from the invoice"
}

IMPORTANT INSTRUCTIONS:
1. Extract ALL line items from the invoice, not just the first few
2. For dates, use YYYY-MM-DD format. If year is 2-digit, assume 20XX
3. For prices, extract as decimal numbers (no dollar signs or commas)
4. If a field is not present on the invoice, use null for strings or 0.00 for numbers
5. For the brand field, extract from the product name (e.g., "CBX", "WYLD", "Highatus")
6. Calculate totals accurately - sum all line item totals should match invoice subtotal
7. Return ONLY the JSON object, no other text

Now extract the data from this invoice:"""

    def _parse_claude_response(self, response_text: str) -> Dict:
        """Parse Claude's response and extract the JSON."""
        try:
            # Try to find JSON in the response
            # Sometimes Claude adds markdown formatting despite instructions
            response_text = response_text.strip()

            # Remove markdown code blocks if present
            if response_text.startswith('```json'):
                response_text = response_text[7:]
            elif response_text.startswith('```'):
                response_text = response_text[3:]

            if response_text.endswith('```'):
                response_text = response_text[:-3]

            response_text = response_text.strip()

            # Parse JSON
            data = json.loads(response_text)
            return data

        except json.JSONDecodeError as e:
            # If JSON parsing fails, try to extract just the JSON object
            import re
            json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
            if json_match:
                try:
                    return json.loads(json_match.group(0))
                except:
                    pass

            raise ValueError(f"Could not parse JSON from Claude response: {e}\nResponse: {response_text[:500]}")

    def extract_from_directory(self, directory_path: str, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Extract invoice data from all PDFs in a directory.

        Args:
            directory_path: Path to directory containing invoice PDFs
            limit: Optional limit on number of files to process

        Returns:
            DataFrame with combined invoice data
        """
        all_invoices = []
        processed_count = 0

        pdf_files = [f for f in os.listdir(directory_path) if f.lower().endswith('.pdf')]

        if limit:
            pdf_files = pdf_files[:limit]

        print(f"Processing {len(pdf_files)} invoice PDFs...")

        for i, filename in enumerate(pdf_files, 1):
            pdf_path = os.path.join(directory_path, filename)

            print(f"\n[{i}/{len(pdf_files)}] Extracting: {filename}")

            try:
                invoice_data = self.extract_from_pdf(pdf_path)

                if 'error' not in invoice_data:
                    all_invoices.append(invoice_data)
                    processed_count += 1

                    # Show preview
                    print(f"  ✓ Invoice: {invoice_data.get('invoice_number', 'N/A')}")
                    print(f"  ✓ Vendor: {invoice_data.get('vendor', 'N/A')}")
                    print(f"  ✓ Total: ${invoice_data.get('invoice_total', 0):,.2f}")
                    print(f"  ✓ Line items: {len(invoice_data.get('line_items', []))}")
                else:
                    print(f"  ✗ Error: {invoice_data.get('error')}")

            except Exception as e:
                self.extraction_errors.append(f"Failed to process {filename}: {str(e)}")
                print(f"  ✗ Exception: {str(e)}")

        print(f"\n{'='*60}")
        print(f"Successfully processed {processed_count}/{len(pdf_files)} invoices")

        if self.extraction_errors:
            print(f"Errors: {len(self.extraction_errors)}")

        # Convert to DataFrame
        if all_invoices:
            return self._invoices_to_dataframe(all_invoices)
        else:
            return pd.DataFrame()

    def _invoices_to_dataframe(self, invoices: List[Dict]) -> pd.DataFrame:
        """
        Convert list of invoice dictionaries to a flat DataFrame.
        Each row is an invoice line item with header info repeated.
        """
        rows = []

        for invoice in invoices:
            # Extract header info
            header_info = {
                'vendor': invoice.get('vendor'),
                'vendor_license': invoice.get('vendor_license'),
                'invoice_number': invoice.get('invoice_number'),
                'invoice_date': invoice.get('invoice_date'),
                'customer_name': invoice.get('customer_name'),
                'customer_license': invoice.get('customer_license'),
                'customer_address': invoice.get('customer_address'),
                'customer_contact': invoice.get('customer_contact'),
                'customer_phone': invoice.get('customer_phone'),
                'delivery_date': invoice.get('delivery_date'),
                'payment_due_date': invoice.get('payment_due_date'),
                'payment_terms': invoice.get('payment_terms'),
                'sales_rep': invoice.get('sales_rep'),
                'invoice_subtotal': invoice.get('invoice_subtotal'),
                'invoice_discount': invoice.get('invoice_discount'),
                'invoice_delivery_fee': invoice.get('invoice_delivery_fee'),
                'invoice_tax': invoice.get('invoice_tax'),
                'invoice_total': invoice.get('invoice_total'),
                'currency': invoice.get('currency', 'USD'),
                'notes': invoice.get('notes'),
                'source_file': invoice.get('source_file'),
                'extracted_at': invoice.get('extracted_at'),
                'extraction_method': invoice.get('extraction_method')
            }

            # Create a row for each line item
            for line_item in invoice.get('line_items', []):
                row = {**header_info, **line_item}
                rows.append(row)

            # If no line items, still create one row with header info
            if not invoice.get('line_items'):
                rows.append(header_info)

        return pd.DataFrame(rows)

    def get_extraction_summary(self, invoices_df: pd.DataFrame) -> Dict:
        """Generate summary statistics from extracted invoice data."""
        if invoices_df.empty:
            return {'error': 'No data to summarize'}

        summary = {
            'total_invoices': invoices_df['invoice_number'].nunique() if 'invoice_number' in invoices_df.columns else 0,
            'total_line_items': len(invoices_df),
            'total_value': invoices_df['invoice_total'].sum() if 'invoice_total' in invoices_df.columns else 0,
            'avg_invoice_value': invoices_df.groupby('invoice_number')['invoice_total'].first().mean() if 'invoice_number' in invoices_df.columns else 0,
            'vendors': invoices_df['vendor'].unique().tolist() if 'vendor' in invoices_df.columns else [],
            'brands': invoices_df['brand'].value_counts().head(10).to_dict() if 'brand' in invoices_df.columns else {},
            'date_range': {
                'earliest': invoices_df['invoice_date'].min() if 'invoice_date' in invoices_df.columns else None,
                'latest': invoices_df['invoice_date'].max() if 'invoice_date' in invoices_df.columns else None
            },
            'extraction_errors': self.extraction_errors,
            'extraction_method': 'Claude Vision API'
        }

        return summary


# =============================================================================
# DEMO/TEST FUNCTIONS
# =============================================================================

def test_single_invoice(pdf_path: str, api_key: Optional[str] = None):
    """Test extraction on a single invoice and display results."""
    print("=" * 80)
    print("INVOICE EXTRACTION TEST")
    print("=" * 80)
    print(f"File: {pdf_path}")
    print()

    extractor = InvoiceExtractor(api_key=api_key)
    result = extractor.extract_from_pdf(pdf_path)

    if 'error' in result:
        print(f"ERROR: {result['error']}")
        return None

    # Display results
    print(f"Invoice Number: {result.get('invoice_number')}")
    print(f"Vendor: {result.get('vendor')}")
    print(f"Date: {result.get('invoice_date')}")
    print(f"Total: ${result.get('invoice_total', 0):,.2f}")
    print(f"Payment Terms: {result.get('payment_terms')}")
    print()
    print(f"Line Items ({len(result.get('line_items', []))}):")
    print("-" * 80)

    for i, item in enumerate(result.get('line_items', []), 1):
        print(f"{i}. {item.get('item_name', 'N/A')}")
        print(f"   Brand: {item.get('brand', 'N/A')} | SKU: {item.get('product_code', 'N/A')}")
        print(f"   Qty: {item.get('quantity', 0)} @ ${item.get('unit_price', 0):.2f} = ${item.get('total_price', 0):.2f}")
        print()

    print("=" * 80)

    # Save JSON
    json_filename = pdf_path.replace('.pdf', '_extracted.json')
    with open(json_filename, 'w') as f:
        json.dump(result, f, indent=2, default=str)
    print(f"Saved detailed JSON to: {json_filename}")

    return result


def batch_extract_invoices(directory_path: str, output_csv: str = "invoices_extracted.csv",
                           api_key: Optional[str] = None, limit: Optional[int] = None):
    """Extract all invoices in a directory and save to CSV."""
    extractor = InvoiceExtractor(api_key=api_key)

    invoices_df = extractor.extract_from_directory(directory_path, limit=limit)

    if not invoices_df.empty:
        # Save to CSV
        invoices_df.to_csv(output_csv, index=False)
        print(f"\n✓ Saved extracted data to: {output_csv}")

        # Print summary
        summary = extractor.get_extraction_summary(invoices_df)
        print(f"\n{'='*60}")
        print("EXTRACTION SUMMARY")
        print('='*60)
        print(f"Total Invoices: {summary['total_invoices']}")
        print(f"Total Line Items: {summary['total_line_items']}")
        print(f"Total Value: ${summary['total_value']:,.2f}")
        print(f"Average Invoice: ${summary['avg_invoice_value']:,.2f}")
        print(f"Vendors: {', '.join(summary['vendors'])}")
        print(f"Date Range: {summary['date_range']['earliest']} to {summary['date_range']['latest']}")

        if summary['brands']:
            print(f"\nTop Brands:")
            for brand, count in list(summary['brands'].items())[:5]:
                print(f"  - {brand}: {count} items")

        if summary['extraction_errors']:
            print(f"\n⚠ Errors ({len(summary['extraction_errors'])}):")
            for error in summary['extraction_errors'][:5]:
                print(f"  - {error}")

        return invoices_df
    else:
        print("\n✗ No invoices extracted")
        return None


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        # Test single invoice
        pdf_path = sys.argv[1]
        test_single_invoice(pdf_path)
    else:
        # Batch extract from invoices directory
        print("Usage:")
        print("  Single invoice: python invoice_extraction.py <path_to_invoice.pdf>")
        print("  Batch extract:  python invoice_extraction.py")
        print()

        invoices_dir = "invoices"
        if os.path.exists(invoices_dir):
            batch_extract_invoices(invoices_dir)
        else:
            print(f"Error: {invoices_dir} directory not found")
