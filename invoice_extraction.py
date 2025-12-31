"""
Invoice Extraction Module - Direct PDF Parsing
Extracts structured data from Treez invoice PDFs using pdfplumber for superior table extraction.
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
import pdfplumber

try:
    import boto3
    from boto3.dynamodb.conditions import Key, Attr
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False


class TreezInvoiceParser:
    """
    Parser for Treez-formatted invoice PDFs using pdfplumber.
    Provides superior table extraction and layout analysis compared to PyPDF2.
    """

    def __init__(self):
        self.extraction_errors = []

    def extract_from_pdf(self, pdf_path: str) -> Dict:
        """
        Extract invoice data from a Treez PDF invoice using pdfplumber.

        Args:
            pdf_path: Path to the PDF invoice file

        Returns:
            Dictionary containing structured invoice data
        """
        try:
            with pdfplumber.open(pdf_path) as pdf:
                # Extract from all pages
                all_text = ""
                all_tables = []

                for page in pdf.pages:
                    all_text += page.extract_text() + "\n"

                    # Extract tables from this page
                    tables = page.extract_tables()
                    if tables:
                        all_tables.extend(tables)

            # Parse the invoice
            invoice_data = self._parse_treez_invoice(all_text, all_tables)

            # Add metadata
            invoice_data['source_file'] = os.path.basename(pdf_path)
            invoice_data['extracted_at'] = datetime.now().isoformat()
            invoice_data['extraction_method'] = 'pdf_parsing'

            # Extract from filename (primary method for Treez invoices)
            # Format: invoice_13608_20251230_173551.pdf
            # Parts: invoice_[NUMBER]_[YYYYMMDD]_[HHMMSS].pdf
            filename = os.path.basename(pdf_path)
            filename_match = re.search(r'invoice[_\s-]*(\d+)_(\d{8})_(\d{6})', filename, re.IGNORECASE)

            if filename_match:
                invoice_num, date_str, time_str = filename_match.groups()

                # Set invoice number (filename is more reliable than PDF text)
                invoice_data['invoice_number'] = invoice_num
                invoice_data['invoice_id'] = invoice_num

                # Parse date from filename if not found in PDF
                if not invoice_data.get('invoice_date'):
                    try:
                        # Convert YYYYMMDD to YYYY-MM-DD
                        year = date_str[:4]
                        month = date_str[4:6]
                        day = date_str[6:8]
                        invoice_data['invoice_date'] = f"{year}-{month}-{day}"
                    except:
                        pass
            else:
                # Fallback: try simpler pattern
                filename_match = re.search(r'invoice[_\s-]*(\d+)', filename, re.IGNORECASE)
                if filename_match:
                    invoice_data['invoice_number'] = filename_match.group(1)
                    invoice_data['invoice_id'] = filename_match.group(1)

            return invoice_data

        except Exception as e:
            self.extraction_errors.append(f"Error extracting {pdf_path}: {str(e)}")
            return {
                'error': str(e),
                'source_file': os.path.basename(pdf_path),
                'extracted_at': datetime.now().isoformat()
            }

    def _parse_treez_invoice(self, text: str, tables: List[List[List[str]]]) -> Dict:
        """Parse Treez invoice format from extracted text and tables."""

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

        # Extract vendor info (top left section - first text after "Print Window")
        # Common patterns: "NABITWO, LLC", "HESHIES LLC", etc.
        vendor_pattern = r'^([A-Z][A-Z\s,\.&]+(?:LLC|INC|CORP)\.?)'
        for i, line in enumerate(lines):
            line_clean = line.strip()
            # Skip header lines
            if line_clean in ['Need Help?', 'Print Window', '']:
                continue
            # Match vendor pattern
            if re.match(vendor_pattern, line_clean, re.IGNORECASE):
                invoice['vendor'] = line_clean
                # Next lines are likely address and license
                if i + 1 < len(lines):
                    invoice['vendor_address'] = lines[i + 1].strip()
                # Look for license in nearby lines
                for j in range(i, min(i + 5, len(lines))):
                    license_match = re.search(r'C\d{2}-\d{7}', lines[j])
                    if license_match:
                        invoice['vendor_license'] = license_match.group(0)
                        break
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
        if not invoice_num_match:
            # Try pattern with more whitespace tolerance
            invoice_num_match = re.search(r'INVOICE[\s#]*(\d+)', text, re.IGNORECASE)

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

        # Extract totals from bottom of invoice
        # Handle both "Subtotal" and "Subt otal" (with space from PDF extraction)
        subtotal_match = re.search(r'Subt?\s*otal\s+\$?([\d,]+\.\d{2})', text, re.IGNORECASE)
        if subtotal_match:
            invoice['invoice_subtotal'] = float(subtotal_match.group(1).replace(',', ''))

        discount_match = re.search(r'Discounts\s+\$?([\d,]+\.\d{2})', text, re.IGNORECASE)
        if discount_match:
            invoice['invoice_discount'] = float(discount_match.group(1).replace(',', ''))

        fees_match = re.search(r'Fees\s+\$?([\d,]+\.\d{2})', text, re.IGNORECASE)
        if fees_match:
            invoice['invoice_fees'] = float(fees_match.group(1).replace(',', ''))

        tax_match = re.search(r'Excise\s*Tax\s+\$?([\d,]+\.\d{2})', text, re.IGNORECASE)
        if tax_match:
            invoice['invoice_tax'] = float(tax_match.group(1).replace(',', ''))

        total_match = re.search(r'Total\s*Cost\s+\$?([\d,]+\.\d{2})', text, re.IGNORECASE)
        if total_match:
            invoice['invoice_total'] = float(total_match.group(1).replace(',', ''))

        payments_match = re.search(r'Payments\s+\$?([\d,]+\.\d{2})', text, re.IGNORECASE)
        if payments_match:
            invoice['payments'] = float(payments_match.group(1).replace(',', ''))

        balance_match = re.search(r'Balance\s+\$?([\d,]+\.\d{2})', text, re.IGNORECASE)
        if balance_match:
            invoice['balance'] = float(balance_match.group(1).replace(',', ''))

        # Extract line items using tables (primary) or text fallback
        invoice['line_items'] = self._extract_line_items_from_tables(tables, text)

        return invoice

    def _extract_line_items_from_tables(self, tables: List[List[List[str]]], text: str) -> List[Dict]:
        """
        Extract line items from pdfplumber tables.
        Falls back to text parsing if tables don't contain the data.
        """
        line_items = []

        # Try table extraction first
        for table in tables:
            if not table or len(table) < 2:
                continue

            # Check if this looks like an invoice items table by examining the first row
            # pdfplumber tables may not have headers - data rows start immediately
            first_row = table[0] if table else []

            # Check if first row has enough columns and looks like data (not just 2-3 columns of UI elements)
            if len(first_row) >= 8:
                # Try to parse this table - _parse_table_rows will validate rows
                items_from_table = self._parse_table_rows(table)
                if items_from_table:
                    line_items.extend(items_from_table)

        # If we got items from tables, return them
        if line_items:
            return line_items

        # Otherwise fall back to text parsing
        return self._extract_line_items_from_text(text)

    def _clean_text(self, text: str) -> str:
        """Remove escape characters and clean text extracted by pdfplumber."""
        if not text:
            return ''

        # Convert to string
        text = str(text)

        # Remove null bytes and other control characters
        text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', text)

        # Remove unicode control characters (like \ue5d2, \uf0d7)
        text = re.sub(r'[\ue000-\uf8ff]', '', text)

        # Replace multiple spaces with single space
        text = re.sub(r'\s+', ' ', text)

        # Strip leading/trailing whitespace
        return text.strip()

    def _parse_table_rows(self, table: List[List[str]]) -> List[Dict]:
        """Parse rows from a pdfplumber table."""
        line_items = []

        # Process all rows (pdfplumber tables don't always have headers)
        for row in table:
            if not row or len(row) < 8:
                continue

            try:
                # Find the column offset - some tables have empty leading columns
                # Look for a cell that starts with a digit (the item number)
                offset = 0
                for i, cell in enumerate(row):
                    clean_cell = self._clean_text(cell or '')
                    if clean_cell and re.match(r'^\d+$', clean_cell):
                        offset = i
                        break

                # Skip if we couldn't find an item number
                if offset == 0 and not re.match(r'^\d+$', self._clean_text(row[0] or '')):
                    continue

                # Check if we have enough columns after the offset
                if len(row) - offset < 11:
                    continue

                # Expected format (with offset applied): [Item#, Brand, Product, Type-Subtype, TraceID, SKU(empty), Units, Cost, Excise, Total, Total w/Excise]
                # Example: ['1', '(1)\nHESHIES', 'THE WHITE [1G]', 'PREROLL -\nFLOWER', '1A4060...', '', '100', '$1.50', '$0.00', '$150.00', '$150.00']

                # Column 0: Item number
                item_num = self._clean_text(row[offset] or '')
                if not item_num or not re.match(r'^\d+$', item_num):
                    continue

                # Column 1: Brand (may have promo indicator like "(1)\nHESHIES")
                brand_cell = self._clean_text(row[offset + 1] or '')
                promo_match = re.match(r'\((\d+)\)\s*(.+)', brand_cell, re.DOTALL)
                if promo_match:
                    promo_indicator = int(promo_match.group(1))
                    brand = self._clean_text(promo_match.group(2))
                else:
                    promo_indicator = 1
                    brand = brand_cell

                # Column 2: Product name
                product = self._clean_text(row[offset + 2] or '')

                # Column 3: Type - Subtype (may have newline)
                type_cell = self._clean_text(row[offset + 3] or '')
                type_match = re.match(r'(PREROLL|BEVERAGE|EXTRACT|FLOWER|CARTRIDGE|EDIBLE|VAPE)\s*-\s*(.+)', type_cell, re.IGNORECASE | re.DOTALL)
                if type_match:
                    prod_type = type_match.group(1).upper()
                    subtype = self._clean_text(type_match.group(2))
                else:
                    prod_type = 'UNKNOWN'
                    subtype = 'UNKNOWN'

                # Column 4: Trace ID
                trace_id = self._clean_text(row[offset + 4] or '')
                if not trace_id or not re.match(r'1A', trace_id):
                    continue

                # Column 5: SKU (usually empty, skip)

                # Column 6: Units
                units_str = str(row[offset + 6] or '').strip()
                try:
                    sku_units = int(units_str)
                except:
                    sku_units = 0

                # Column 7: Unit Cost
                cost_str = str(row[offset + 7] or '').replace('$', '').replace(',', '').strip()
                try:
                    unit_cost = float(cost_str)
                except:
                    unit_cost = 0.0

                # Column 8: Excise per unit
                excise_str = str(row[offset + 8] or '').replace('$', '').replace(',', '').strip()
                try:
                    excise = float(excise_str)
                except:
                    excise = 0.0

                # Column 9: Total Cost
                total_str = str(row[offset + 9] or '').replace('$', '').replace(',', '').strip()
                try:
                    total_cost = float(total_str)
                except:
                    total_cost = 0.0

                # Column 10: Total Cost w/ Excise
                total_w_excise_str = str(row[offset + 10] or '').replace('$', '').replace(',', '').strip()
                try:
                    total_w_excise = float(total_w_excise_str)
                except:
                    total_w_excise = total_cost

                # Determine if promo
                is_promo = promo_indicator > 1 or '[PROMO]' in product

                # Extract size from product name
                size_match = re.search(r'(\d+\.?\d*\s*[MG]+)', product, re.IGNORECASE)
                if not size_match:
                    size_match = re.search(r'(\d+\.?\d*[GM])', product, re.IGNORECASE)
                unit_size = size_match.group(1).strip() if size_match else None

                # Extract strain
                strain = None
                strain_match = re.match(r'^([A-Z][A-Z\s\'\-]+?)(?:\[|$)', product)
                if strain_match:
                    strain = strain_match.group(1).strip()

                line_item = {
                    'line_number': int(item_num),
                    'brand': brand,
                    'product_name': product,
                    'product_type': prod_type,
                    'product_subtype': subtype,
                    'strain': strain,
                    'unit_size': unit_size,
                    'trace_id': trace_id,
                    'sku_units': sku_units,
                    'unit_cost': unit_cost,
                    'excise_per_unit': excise,
                    'total_cost': total_cost,
                    'total_cost_with_excise': total_w_excise,
                    'is_promo': is_promo
                }

                line_items.append(line_item)

            except Exception as e:
                # Skip problematic rows
                continue

        return line_items

    def _extract_line_items_from_text(self, text: str) -> List[Dict]:
        """Extract line items from invoice text (fallback method)."""
        return self._extract_line_items(text)

    def _extract_line_items(self, text: str) -> List[Dict]:
        """Extract line items from invoice text."""
        line_items = []

        lines = text.split('\n')

        # Detect which format we're dealing with
        # Format 1: All data on single line ("Item # Brand Product..." header on one line)
        # Format 2: Data spans multiple lines ("Item" on one line, "#Brand..." on next)

        single_line_format = False
        multi_line_format = False

        for i, line in enumerate(lines):
            if 'Item #' in line and 'Brand' in line and 'Product' in line:
                single_line_format = True
                break
            if 'Item' in line and i + 1 < len(lines) and '#Brand' in lines[i + 1]:
                multi_line_format = True
                break

        if single_line_format:
            return self._extract_line_items_single_line(lines)
        elif multi_line_format:
            return self._extract_line_items_multi_line(lines)
        else:
            return []

    def _extract_line_items_single_line(self, lines: List[str]) -> List[Dict]:
        """Extract line items when all data is on a single line per item."""
        line_items = []

        # Find bounds
        start_idx = -1
        end_idx = len(lines)

        for i, line in enumerate(lines):
            if 'Item #' in line and 'Brand' in line:
                start_idx = i + 1  # Items start on next line
                continue
            if start_idx > 0 and re.match(r'^(Fees|Discounts|Subt?\s*otal|Excise Tax|Total Cost)\s+\$', line):
                end_idx = i
                break

        if start_idx < 0:
            return []

        # Parse each line as a complete item
        for i in range(start_idx, end_idx):
            line = lines[i].strip()
            if not line:
                continue

            # Pattern for single-line format:
            # "1 (1) 8 TRACK MAUI WOWIE [1G] PREROLL - FLOWER 1A4060300048D3D003755403 100 $1.49 $0.00 $149.00 $149.00"
            pattern = r'^(\d+)\s+\((\d+)\)\s+(.+?)\s+(PREROLL|BEVERA\s*GE|EXTRACT|FLOWER|CARTRIDGE|EDIBLE|VAPE)\s+-\s+([A-Z\s]+?)\s+(1A[A-Z0-9]+)\s+(\d+)\s+\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})'

            match = re.search(pattern, line)
            if not match:
                continue

            item_num = match.group(1)
            promo_indicator = match.group(2)
            brand_product = match.group(3).strip()
            prod_type = match.group(4).replace(' ', '')
            subtype = match.group(5).strip()
            trace_id = match.group(6)
            units = match.group(7)
            cost = match.group(8)
            excise = match.group(9)
            total_cost = match.group(10)
            total_w_excise = match.group(11)

            # Split brand and product
            # Example: "8 TRA CK MAUI WOWIE [1G]"
            # Brand is typically first 1-4 words before the main product description
            brand_match = re.match(r'^([A-Z0-9\s\.&\'\-]{2,40}?)\s+(.+)$', brand_product)
            if brand_match:
                brand = brand_match.group(1).strip()
                product = brand_match.group(2).strip()
            else:
                brand = 'UNKNOWN'
                product = brand_product

            # Clean up spacing
            brand = re.sub(r'\s+', ' ', brand)
            product = re.sub(r'\s+', ' ', product)

            # Determine promo status
            is_promo = '[PROMO]' in product or int(promo_indicator) > 1

            # Extract size
            size_match = re.search(r'(\d+\.?\d*\s*[MG]+)', product, re.IGNORECASE)
            if not size_match:
                size_match = re.search(r'(\d+\.?\d*[GM])', product, re.IGNORECASE)
            unit_size = size_match.group(1).strip() if size_match else None

            # Extract strain
            strain = None
            strain_match = re.match(r'^([A-Z][A-Z\s\'\-]+?)(?:\[|$)', product)
            if strain_match:
                strain = strain_match.group(1).strip()

            line_item = {
                'line_number': int(item_num),
                'brand': brand,
                'product_name': product,
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

    def _extract_line_items_multi_line(self, lines: List[str]) -> List[Dict]:
        """Extract line items when data spans multiple lines per item."""
        line_items = []

        # Find the line item section bounds
        start_idx = -1
        end_idx = len(lines)

        for i, line in enumerate(lines):
            # Start when we see the table header (use FIRST occurrence only)
            # "Item" on one line, "#Brand Product..." on next line
            if start_idx < 0 and 'Item' in line and i + 1 < len(lines) and '#Brand' in lines[i + 1]:
                # Skip header rows: "Item", "#Brand...", "unitTotal", "CostTotal...", "Excise"
                start_idx = i + 5
                continue
            # Find the LAST occurrence of financial totals (end of all items across all pages)
            # Update end_idx each time we see totals (the last one wins)
            if start_idx > 0 and re.match(r'^(Fees|Discounts|Subt?\s*otal|Excise Tax|Total Cost|Payments|Balance)\s+\$', line):
                end_idx = i

        if start_idx < 0:
            return []

        # Parse line items (they span multiple lines)
        i = start_idx
        while i < end_idx:
            line = lines[i].strip()

            # Skip empty lines and page breaks
            if not line:
                i += 1
                continue

            # Skip page break headers (repeated "Need Help?", "Print Window", "Item", "#Brand", etc.)
            if 'Need Help?' in line or 'Print Window' in line:
                # Skip until we get past the repeated header (usually 5-6 lines)
                while i < end_idx and (not lines[i].strip() or
                                      'Need Help?' in lines[i] or
                                      'Print Window' in lines[i] or
                                      'Item' in lines[i] or
                                      '#Brand' in lines[i] or
                                      'unitTotal' in lines[i] or
                                      'CostTotal' in lines[i] or
                                      'Excise' == lines[i].strip()):
                    i += 1
                continue

            # Match item number and beginning of brand
            # Pattern: "1(4) SL UGGERS" or "1 (5) ST . IDES" or "2NOT YOUR" (no parens)
            item_start_match = re.match(r'^(\d+)\s*(?:\((\d+)\))?\s*(.+)$', line)
            if not item_start_match:
                i += 1
                continue

            item_num = item_start_match.group(1)
            promo_indicator = item_start_match.group(2) or '1'  # Default to '1' if no parens
            brand_part1 = item_start_match.group(3).strip()

            # Collect subsequent lines until we find the pricing data
            product_lines = [brand_part1]
            i += 1

            # Keep reading lines until we find one with pricing pattern
            # The pricing data (Trace ID + prices) is on the last line
            # We need to check combined text since type/subtype may be on previous line
            pricing_match = None
            while i < end_idx:
                next_line = lines[i].strip()

                # Check if we hit the next item (starts with digit, optionally with parentheses)
                # Pattern: "2NOT YOUR" or "2(5) NOT YOUR" or "10 ST IDES"
                next_item_match = re.match(r'^\d+\s*(?:\(\d+\))?\s*[A-Z]', next_line)
                if next_item_match and len(product_lines) > 0:
                    # We've gone too far, back up
                    # Don't include this line
                    break

                product_lines.append(next_line)

                # Try to match pricing pattern on the combined last 2-3 lines
                # (type/subtype might be on previous line, pricing on current line)
                combined_text = ''.join(product_lines[-3:])  # Join last 3 lines

                # Look for pricing pattern: Trace ID + 4 dollar amounts at the end
                pricing_match = re.search(
                    r'(PREROLL|BEVERA\s*GE|EXTRACT|FLOWER|CARTRIDGE|EDIBLE|VAPE).*?'  # Product type (anywhere in combined text)
                    r'(1A[A-Z0-9]+)\s+'  # Trace ID
                    r'(\d+)\s+'  # Units
                    r'\$?([\d,]+\.\d{2})\s+'  # Cost
                    r'\$?([\d,]+\.\d{2})\s+'  # Excise
                    r'\$?([\d,]+\.\d{2})\s+'  # Total Cost
                    r'\$?([\d,]+\.\d{2})\s*$',  # Total w/ Excise
                    combined_text
                )

                if pricing_match:
                    i += 1
                    break

                i += 1

            if not pricing_match:
                # Couldn't find pricing, skip this item
                continue

            prod_type = pricing_match.group(1).replace(' ', '')  # "BEVERA GE" -> "BEVERAGE"
            trace_id = pricing_match.group(2)
            units = pricing_match.group(3)
            cost = pricing_match.group(4)
            excise = pricing_match.group(5)
            total_cost = pricing_match.group(6)
            total_w_excise = pricing_match.group(7)

            # Extract subtype from the text before trace ID
            # Common pattern: "PREROLL - INFUSED" or "BEVERAGE - SODA"
            subtype_match = re.search(r'(?:PREROLL|BEVERA\s*GE|EXTRACT|FLOWER|CARTRIDGE|EDIBLE|VAPE)\s*-\s*([A-Z\s]+?)(?:1A|$)', combined_text)
            subtype = subtype_match.group(1).strip() if subtype_match else 'UNKNOWN'

            # Reconstruct brand and product name from collected lines
            # Remove the pricing portion (everything from Trace ID onward)
            text_before_pricing = combined_text[:pricing_match.start(2)].strip()  # Everything before Trace ID

            # Common structure:
            # Line 1: "SL UGGERS"  (brand part 1)
            # Line 2: "HITHURRICANE SZN [HASH & DIAMOND]"  (brand part 2 + product start)
            # Line 3: "[INFUSED] 1.5GPREROLL -"  (product continuation + type)

            # Clean up the text (remove extra spaces from PDF extraction artifacts)
            text_clean = re.sub(r'\s+', ' ', text_before_pricing)

            # Remove product type keyword if it appears (PREROLL, BEVERAGE, etc.)
            text_clean = re.sub(r'(PREROLL|BEVERA\s*GE|EXTRACT|FLOWER|CARTRIDGE|EDIBLE|VAPE)\s*-?\s*$', '', text_clean, flags=re.IGNORECASE).strip()

            # Try to split brand and product
            # Brand is typically 1-4 words at start (all caps, may have special chars)
            # Examples: "SL UGGERS HIT", "NOT YOUR FATHER'S", "PABST BLUE RIBBON", "ST IDES"
            brand_match = re.match(r'^([A-Z\s\.&\'\-]{2,40}?(?:HIT|FATHER\'S|RIBBON|IDES)?)\s+(.+)$', text_clean)
            if brand_match:
                brand = brand_match.group(1).strip()
                product = brand_match.group(2).strip()
            else:
                # Fallback: first collected line is brand, rest is product
                brand = product_lines[0] if product_lines else 'UNKNOWN'
                product = ' '.join(product_lines[1:-1]) if len(product_lines) > 2 else text_clean

            # Clean up spacing artifacts from PDF extraction
            brand = re.sub(r'\s+', ' ', brand).strip()
            product = re.sub(r'\s+', ' ', product).strip()

            # Determine if promo item
            is_promo = '[PROMO]' in product or 'PROMO' in product or int(promo_indicator) > 1

            # Extract size/quantity from product name
            size_match = re.search(r'(\d+\.?\d*\s*[MG]+)\s*(?:THC)?', product, re.IGNORECASE)
            if not size_match:
                size_match = re.search(r'(\d+\.?\d*[GM])\s', product, re.IGNORECASE)
            unit_size = size_match.group(1).strip() if size_match else None

            # Extract strain/flavor (text before brackets or descriptors)
            strain = None
            strain_match = re.match(r'^([A-Z][A-Z\s\'\-]+?)(?:\[|$)', product)
            if strain_match:
                strain = strain_match.group(1).strip()

            line_item = {
                'line_number': int(item_num),
                'brand': brand,
                'product_name': product,
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
