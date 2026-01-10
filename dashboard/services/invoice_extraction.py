"""
Invoice Extraction Module - Direct PDF Parsing
Extracts structured data from Treez invoice PDFs using pdfplumber for superior table extraction
and PyMuPDF (fitz) for text extraction (better font handling for dates).
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

# PyMuPDF for better text extraction (handles problematic fonts that render as null bytes)
try:
    import fitz  # PyMuPDF
    PYMUPDF_AVAILABLE = True
except ImportError:
    PYMUPDF_AVAILABLE = False

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

    # Pre-compiled regex patterns for better performance
    _RE_ITEM_NUM = re.compile(r'^\d+$')
    _RE_INVOICE_FILENAME = re.compile(r'invoice[_\s-]*(\d+)_(\d{8})_(\d{6})', re.IGNORECASE)
    _RE_INVOICE_FILENAME_SIMPLE = re.compile(r'invoice[_\s-]*(\d+)', re.IGNORECASE)
    _RE_TYPE_KEYWORD = re.compile(r'^(CARTRIDGE|EDIBLE|PREROLL|FLOWER|EXTRACT|BEVERAGE|TINCTURE|TOPICAL|CAPSULE|CONCENTRATE|VAPE|MERCH|MERCHANDISE|ACCESSORY|SUPPLY|SUPPLIES|ROLLING|INFUSED|APPAREL|GEAR|DEVICE|HARDWARE|PILL|TABLET|SUBLINGUAL|OIL|SPRAY|PATCH|BALM|LOTION|CREAM)\s*-', re.IGNORECASE)
    _RE_TRACE_ID = re.compile(r'^[A-Z0-9]{20,}')
    _RE_TOTALS = re.compile(r'^(Fees|Discounts|Subtotal|Subt\s*otal|Total\s*Cost|Excise\s*Tax|Payments|Balance)', re.IGNORECASE)
    _RE_PRICE = re.compile(r'^\d+[\d,]*\.\d{2}$')
    _RE_SIZE = re.compile(r'(\d+\.?\d*\s*[MG]+)', re.IGNORECASE)

    def __init__(self):
        self.extraction_errors = []

    def extract_from_pdf(self, pdf_path: str) -> Dict:
        """
        Extract invoice data from a Treez PDF invoice.
        Uses PyMuPDF for text extraction (better font handling) and pdfplumber for tables.

        Args:
            pdf_path: Path to the PDF invoice file

        Returns:
            Dictionary containing structured invoice data
        """
        try:
            # Extract text using PyMuPDF (handles problematic fonts that render as null bytes)
            # PyMuPDF is ~4x faster than pdfplumber for text extraction
            all_text = ""
            if PYMUPDF_AVAILABLE:
                try:
                    doc = fitz.open(pdf_path)
                    for page in doc:
                        all_text += page.get_text() + "\n"
                    doc.close()
                except Exception as e:
                    # Fall back to pdfplumber if PyMuPDF fails
                    all_text = ""

            # OPTIMIZATION: Try fast text-based line item extraction first (~5ms)
            # Only fall back to pdfplumber table extraction (~300ms) if text parsing fails
            line_items_from_text = self._extract_line_items_from_pymupdf_text(all_text)

            all_tables = []
            if not line_items_from_text:
                # Fall back to pdfplumber for table extraction (slower but more reliable)
                with pdfplumber.open(pdf_path) as pdf:
                    # Only use pdfplumber for text if PyMuPDF wasn't available or failed
                    if not all_text:
                        for page in pdf.pages:
                            all_text += (page.extract_text() or "") + "\n"

                    # Extract tables from all pages (pdfplumber excels at this)
                    for page in pdf.pages:
                        tables = page.extract_tables()
                        if tables:
                            all_tables.extend(tables)

            # Parse the invoice (will use pre-extracted line items if available)
            invoice_data = self._parse_treez_invoice(all_text, all_tables, line_items_from_text)

            # Add metadata
            invoice_data['source_file'] = os.path.basename(pdf_path)
            invoice_data['extracted_at'] = datetime.now().isoformat()
            invoice_data['extraction_method'] = 'pdf_parsing'

            # Extract from filename (for invoice number and download date)
            # Format: invoice_13608_20251230_173551.pdf
            # Parts: invoice_[NUMBER]_[YYYYMMDD]_[HHMMSS].pdf
            filename = os.path.basename(pdf_path)
            filename_match = self._RE_INVOICE_FILENAME.search(filename)

            if filename_match:
                invoice_num, date_str, time_str = filename_match.groups()

                # ALWAYS use invoice number from filename - it's the authoritative source
                # The filename format is: invoice_[NUMBER]_[YYYYMMDD]_[HHMMSS].pdf
                # PDF content may contain different numbers (e.g., internal reference numbers)
                invoice_data['invoice_number'] = invoice_num
                invoice_data['invoice_id'] = invoice_num

                # Parse download date from filename (when invoice was downloaded/exported)
                try:
                    year = date_str[:4]
                    month = date_str[4:6]
                    day = date_str[6:8]
                    invoice_data['download_date'] = f"{year}-{month}-{day}"
                except:
                    pass

                # Use filename date as fallback for invoice_date ONLY if PDF extraction didn't find a date
                # BUT NOT if we detected a date that couldn't be read (null byte issue)
                # (invoice_date should come from "Created:" in PDF header)
                if not invoice_data.get('invoice_date') and not invoice_data.get('_date_extraction_failed'):
                    invoice_data['invoice_date'] = invoice_data.get('download_date')
            else:
                # Fallback: try simpler pattern for invoice number only
                # Filename is always authoritative for invoice number
                filename_match = self._RE_INVOICE_FILENAME_SIMPLE.search(filename)
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

    def _extract_vendor_from_header(self, text: str) -> Optional[str]:
        """
        Extract vendor name from PyMuPDF text output.
        PyMuPDF extracts text line-by-line, so vendor appears on its own line
        after "Print Window" and before "Barbary Coast Dispensary".

        Header structure (PyMuPDF output):
          Line 0: "Need Help?"
          Line 1: "menu"
          Line 2: "Print Window"
          Line 3: VENDOR NAME (e.g., "NABITWO, LLC") - OR "Barbary Coast Dispensary" if no vendor
          Line 4+: Address, status, dates...
        """
        lines = [l.strip() for l in text.split('\n') if l.strip()]

        # Find "Print Window" line
        print_window_idx = -1
        for i, line in enumerate(lines):
            if 'Print Window' in line:
                print_window_idx = i
                break

        if print_window_idx < 0 or print_window_idx + 1 >= len(lines):
            return None

        # The line after "Print Window" should be the vendor name
        # Unless it's "Barbary Coast Dispensary" (receiver) which means no vendor listed
        potential_vendor = lines[print_window_idx + 1]

        # Skip if it's the receiver name (Barbary Coast or Grass Roots)
        if 'Barbary Coast' in potential_vendor or 'Grass Roots' in potential_vendor:
            return None

        # Skip if it looks like an address (starts with number or contains common address words)
        if re.match(r'^\d+\s', potential_vendor) or 'Mission St' in potential_vendor:
            return None

        # Skip UI elements that might appear
        skip_patterns = ['Need Help', 'menu', 'FULFILLED', 'PENDING', 'CANCELLED', 'Created:', 'INVOICE']
        for pattern in skip_patterns:
            if pattern in potential_vendor:
                return None

        # Clean up vendor name
        vendor = potential_vendor.strip()

        # Handle multi-line vendor names (e.g., "NABITWO, LLC" followed by address on same conceptual line)
        # Check if next line is a continuation like "INC" or "LLC"
        if print_window_idx + 2 < len(lines):
            next_line = lines[print_window_idx + 2].strip()
            if next_line in ['INC', 'LLC', 'CORP', 'INC.', 'LLC.', 'CORP.']:
                vendor = vendor.rstrip(',').strip() + ' ' + next_line

        return vendor if vendor else None

    def _parse_treez_invoice(self, text: str, tables: List[List[List[str]]], preextracted_line_items: List[Dict] = None) -> Dict:
        """Parse Treez invoice format from extracted text and tables."""

        # Detect non-Treez invoice formats (e.g., inventory receiving system)
        # These have different header structures that we can't parse properly
        if self._is_non_treez_format(text):
            return {
                'error': 'Non-Treez invoice format detected (inventory/receiving system)',
                'format_type': 'inventory_receiving'
            }

        # Initialize invoice data
        invoice = {
            'distributor': None,
            'distributor_address': None,
            'distributor_license': None,
            'receiver': None,
            'receiver_address': None,
            'invoice_status': None,
            'invoice_number': None,
            'invoice_id': None,
            'invoice_date': None,
            'accepted_date': None,
            'created_by': None,
            'payment_terms': None,
            # Keep legacy field names for compatibility
            'vendor': None,
            'vendor_license': None,
            'vendor_address': None,
            'customer_name': None,
            'customer_address': None,
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

        # =================================================================
        # HEADER EXTRACTION - Parse the 4-column header structure
        # =================================================================
        # The Treez invoice header has 4 columns that get merged in text extraction:
        # Col 1: Distributor (name, address, phone, license)
        # Col 2: Receiver (always Barbary Coast or Grass Roots + address)
        # Col 3: Status info (FULFILLED, Created date, Accepted date, Created by, Payment terms)
        # Col 4: Invoice # and secondary ID
        #
        # Example merged line:
        # "DOS ZAPATOS , INC. Barbary Coast Dispensary FULFILLED INVOICE#"
        # =================================================================

        # Find the first content line after "Print Window" which contains the merged header
        header_start_idx = -1
        for i, line in enumerate(lines):
            if 'Print Window' in line:
                header_start_idx = i + 1
                break

        if header_start_idx >= 0 and header_start_idx < len(lines):
            # Process the merged header lines
            # The header typically spans 5-8 lines before "Item #" table header
            header_lines = []
            for i in range(header_start_idx, min(header_start_idx + 15, len(lines))):
                line = lines[i].strip()
                if 'Item #' in line or 'Item' == line:
                    break
                if line and line not in ['Need', 'Help?', 'Need Help?']:
                    header_lines.append(line)

            # Parse the merged header - PyMuPDF extracts line-by-line
            # Format 1 (no vendor): "Grass Roots" on first line, address on second
            # Format 2 (with vendor): Vendor on first line(s), then "Barbary Coast Dispensary" or "Grass Roots"
            if header_lines:
                first_line = header_lines[0]

                # Check if first line is just the receiver name (no vendor format)
                if first_line.strip() == 'Grass Roots':
                    invoice['receiver'] = 'Grass Roots'
                    # Address should be on next line: "1077 Post St. San Francisco CA 94109"
                    if len(header_lines) > 1:
                        addr_line = header_lines[1]
                        if 'Post St' in addr_line:
                            invoice['receiver_address'] = addr_line.strip()
                    # Status and other info on subsequent lines
                    for line in header_lines[2:10]:
                        if 'FULFILLED' in line:
                            invoice['invoice_status'] = 'FULFILLED'
                        elif 'PENDING' in line:
                            invoice['invoice_status'] = 'PENDING'
                        elif 'CANCELLED' in line:
                            invoice['invoice_status'] = 'CANCELLED'
                elif first_line.strip() == 'Barbary Coast Dispensary':
                    invoice['receiver'] = 'Barbary Coast Dispensary'
                    if len(header_lines) > 1:
                        addr_line = header_lines[1]
                        if 'Mission St' in addr_line:
                            invoice['receiver_address'] = addr_line.strip()
                    for line in header_lines[2:10]:
                        if 'FULFILLED' in line:
                            invoice['invoice_status'] = 'FULFILLED'
                        elif 'PENDING' in line:
                            invoice['invoice_status'] = 'PENDING'
                        elif 'CANCELLED' in line:
                            invoice['invoice_status'] = 'CANCELLED'
                else:
                    # First line is likely vendor name - search for receiver in subsequent lines
                    # Build vendor name from lines until we find receiver
                    vendor_parts = []
                    receiver_line_idx = -1

                    for idx, line in enumerate(header_lines):
                        line_clean = line.strip()
                        if line_clean == 'Grass Roots':
                            invoice['receiver'] = 'Grass Roots'
                            receiver_line_idx = idx
                            break
                        elif line_clean == 'Barbary Coast Dispensary':
                            invoice['receiver'] = 'Barbary Coast Dispensary'
                            receiver_line_idx = idx
                            break
                        elif 'Grass Roots' in line_clean or 'Barbary Coast' in line_clean:
                            # Mixed on one line - try to extract
                            for store in ['Barbary Coast Dispensary', 'Grass Roots']:
                                if store in line_clean:
                                    invoice['receiver'] = store
                                    # Everything before is vendor
                                    vendor_part = line_clean.split(store)[0].strip().rstrip(',').strip()
                                    if vendor_part:
                                        vendor_parts.append(vendor_part)
                                    receiver_line_idx = idx
                                    break
                            if invoice['receiver']:
                                break
                        # Otherwise accumulate as vendor name (skip address-like lines)
                        elif not re.match(r'^\d+\s', line_clean) and 'Mission St' not in line_clean and 'Post St' not in line_clean:
                            # Skip status, dates, etc
                            if line_clean not in ['FULFILLED', 'PENDING', 'CANCELLED'] and not line_clean.startswith('Created:'):
                                if not re.match(r'^[\(\d]', line_clean):  # Skip phone/zip
                                    if not '@' in line_clean and not 'LIC' in line_clean:  # Skip email/license
                                        vendor_parts.append(line_clean)

                    # Build vendor name from parts (usually first 1-2 lines)
                    if vendor_parts:
                        # Take first 2 lines max for vendor name
                        vendor_name = ' '.join(vendor_parts[:2]).strip()
                        # Clean up common issues
                        vendor_name = re.sub(r'\s+', ' ', vendor_name)
                        if vendor_name and vendor_name not in ['FULFILLED', 'PENDING', 'CANCELLED']:
                            invoice['distributor'] = vendor_name

                    # Extract receiver address from line after receiver
                    if receiver_line_idx >= 0 and receiver_line_idx + 1 < len(header_lines):
                        addr_line = header_lines[receiver_line_idx + 1]
                        if 'Mission St' in addr_line or 'Post St' in addr_line:
                            invoice['receiver_address'] = addr_line.strip()

                    # Find status in remaining lines
                    for line in header_lines:
                        if 'FULFILLED' in line:
                            invoice['invoice_status'] = 'FULFILLED'
                            break
                        elif 'PENDING' in line:
                            invoice['invoice_status'] = 'PENDING'
                            break
                        elif 'CANCELLED' in line:
                            invoice['invoice_status'] = 'CANCELLED'
                            break

                # Extract addresses from second header line
                # Pattern: "ADDRESS1 952 Mission St, San Francisco, CA Created: MM/DD/YYYY INVOICE_NUM"
                # OR: "INC 952 Mission St..." when vendor name continues
                if len(header_lines) > 1:
                    second_line = header_lines[1]

                    # Check if second line starts with INC/LLC/CORP (continuation of vendor name)
                    # Example: "INC 952 Mission St, San Francisco, CA Created: 04/03/2023 12277"
                    name_continuation = re.match(r'^(INC|LLC|CORP)\b\s*', second_line.strip(), re.IGNORECASE)
                    if name_continuation:
                        # Append to distributor name
                        if invoice.get('distributor'):
                            invoice['distributor'] = invoice['distributor'].rstrip(',').strip() + ' ' + name_continuation.group(1).upper()
                        # Remove this prefix from the line for address parsing
                        second_line = second_line[name_continuation.end():].strip()

                    # Find receiver address (Mission St pattern - may have spaces instead of numbers in some PDFs)
                    receiver_addr_match = re.search(
                        r'(\d*\s*Mission\s+St,?\s*San\s+Francisco,?\s*CA)',
                        second_line,
                        re.IGNORECASE
                    )
                    if receiver_addr_match:
                        addr = receiver_addr_match.group(1).strip()
                        # If no number found or just spaces, use the standard address
                        if not re.match(r'\d', addr.strip()):
                            addr = '952 Mission St, San Francisco, CA'
                        invoice['receiver_address'] = addr
                        # Everything before this is likely distributor address
                        dist_addr_part = second_line[:receiver_addr_match.start()].strip()
                        # Only set if it looks like a real address (has digits AND street keywords)
                        # Must have actual content, not just "952 Mission St" which is receiver address
                        if dist_addr_part and re.search(r'\d', dist_addr_part):
                            # Make sure it's not just the Mission St address
                            if not re.search(r'^9?5?2?\s*Mission', dist_addr_part, re.IGNORECASE):
                                if re.search(r'(ST|STREET|AVE|BLVD|RD|DR|WAY|LN|HIGHWAY|HWY|UNIT|SUITE)', dist_addr_part, re.IGNORECASE):
                                    invoice['distributor_address'] = dist_addr_part

                    # Extract invoice number from end of line (after INVOICE#)
                    inv_num_match = re.search(r'(\d{4,6})\s*$', second_line)
                    if inv_num_match:
                        invoice['invoice_number'] = inv_num_match.group(1)
                        invoice['invoice_id'] = inv_num_match.group(1)

                # If receiver address not found, try alternate patterns or set default
                if not invoice['receiver_address']:
                    # Look for any Mission St or Post St reference in all header lines
                    for line in header_lines:
                        if 'Mission St' in line or 'Mission' in line:
                            invoice['receiver_address'] = '952 Mission St, San Francisco, CA 94103'
                            break
                        if 'Post St' in line:
                            invoice['receiver_address'] = '1077 Post St, San Francisco, CA 94109'
                            break
                    # If still not found, set default based on receiver
                    if not invoice['receiver_address']:
                        if invoice['receiver'] == 'Barbary Coast Dispensary':
                            invoice['receiver_address'] = '952 Mission St, San Francisco, CA 94103'
                        elif invoice['receiver'] == 'Grass Roots':
                            invoice['receiver_address'] = '1077 Post St, San Francisco, CA 94109'

                # Look for additional distributor info in subsequent lines
                for idx, line in enumerate(header_lines[2:8]):
                    # Look for zip code line (distributor address continuation)
                    if re.match(r'^\d{5}', line) and invoice.get('distributor_address'):
                        # Append zip code to address
                        invoice['distributor_address'] += f" {line.split()[0]}"
                        continue

                    # Look for phone number
                    phone_match = re.search(r'\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}', line)
                    if phone_match:
                        if invoice.get('distributor_address'):
                            invoice['distributor_address'] += f" {phone_match.group(0)}"
                        else:
                            # Phone might help identify address
                            pass

                    # Look for license number
                    license_match = re.search(r'C\d{2}-\d{7}', line)
                    if license_match:
                        invoice['distributor_license'] = license_match.group(0)

                # Try to extract distributor address from merged text if still missing
                if not invoice.get('distributor_address') and len(header_lines) > 1:
                    # Look for address patterns like "5733 SAN LEANDRO ST" or street addresses
                    for line in header_lines[1:4]:
                        # Look for street address patterns with numbers
                        # But exclude Mission St which is always the receiver address
                        addr_match = re.search(r'(\d+\s+[A-Z\s]+(?:ST|STREET|AVE|AVENUE|BLVD|RD|ROAD|DR|DRIVE|WAY|LN|LANE|HIGHWAY|HWY))',
                                              line, re.IGNORECASE)
                        if addr_match:
                            potential_addr = addr_match.group(1).strip()
                            # Skip if this is Mission St (receiver address)
                            if not re.search(r'Mission\s*St', potential_addr, re.IGNORECASE):
                                invoice['distributor_address'] = potential_addr
                                break
                        # Also check for patterns with suite/unit (numbers may be spaces due to PDF rendering)
                        suite_match = re.search(r'(\s*[A-Z\s]+(?:ST|STREET)\.?\s+SUITE\s+[A-Z0-9&]+)', line, re.IGNORECASE)
                        if suite_match:
                            addr = suite_match.group(1).strip()
                            # Clean up leading spaces that might be where numbers should be
                            addr = re.sub(r'^\s+', '', addr)
                            # Skip Mission St
                            if not re.search(r'Mission\s*St', addr, re.IGNORECASE):
                                invoice['distributor_address'] = addr
                                break
                        # Check for street names without numbers (PDF rendering may drop numbers)
                        street_only = re.search(r'([A-Z]+\s+(?:LEANDRO|MAIN|FIRST|SECOND|OAK|ELM|PARK)\s*ST\.?\s*(?:SUITE\s+[A-Z0-9&]+)?)',
                                               line, re.IGNORECASE)
                        if street_only:
                            invoice['distributor_address'] = street_only.group(1).strip()
                            break

                # Also look for license in all header lines (format: C11-0001274)
                if not invoice.get('distributor_license'):
                    for line in header_lines:
                        license_match = re.search(r'C\d{1,2}-?\d{5,7}', line)
                        if license_match:
                            invoice['distributor_license'] = license_match.group(0)
                            break

        # Extract invoice status if not found
        if not invoice['invoice_status']:
            if 'FULFILLED' in text:
                invoice['invoice_status'] = 'FULFILLED'
            elif 'PENDING' in text:
                invoice['invoice_status'] = 'PENDING'
            elif 'CANCELLED' in text:
                invoice['invoice_status'] = 'CANCELLED'

        # Extract invoice number if not found
        if not invoice['invoice_number']:
            invoice_num_match = re.search(r'INVOICE#?\s*\n?\s*(\d+)', text)
            if not invoice_num_match:
                invoice_num_match = re.search(r'INVOICE[\s#]*(\d{4,6})', text, re.IGNORECASE)
            if invoice_num_match:
                invoice['invoice_number'] = invoice_num_match.group(1)
                invoice['invoice_id'] = invoice_num_match.group(1)

        # Extract dates
        # Primary pattern: normal digits
        date_pattern = r'Created:\s*(\d{1,2})/(\d{1,2})/(\d{4})'
        date_match = re.search(date_pattern, text)
        if date_match:
            month, day, year = date_match.groups()
            invoice['invoice_date'] = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
        else:
            # Check if date has null bytes (font rendering issue)
            # Pattern: Created: followed by null bytes and slashes like \x00\x00/\x00\x00/\x00\x00\x00\x00
            null_date_pattern = r'Created:\s*[\x00\s]*[/\x00\s]+[\x00\s]*[/\x00\s]+[\x00\s]*'
            if re.search(null_date_pattern, text):
                # Mark that we detected a date but couldn't read it (null byte issue)
                invoice['_date_extraction_failed'] = True

        # Extract accepted date
        accepted_pattern = r'Accepted:\s*\n?\s*(\d{1,2})/(\d{1,2})/(\d{4})'
        accepted_match = re.search(accepted_pattern, text)
        if accepted_match:
            month, day, year = accepted_match.groups()
            invoice['accepted_date'] = f"{year}-{month.zfill(2)}-{day.zfill(2)}"

        # Extract created by
        created_by_match = re.search(r'Created by:\s*([\w\s]+?)(?:\n|$)', text)
        if created_by_match:
            created_by = created_by_match.group(1).strip()
            # Remove common suffixes that get appended
            created_by = re.sub(r'\s*(undefined|COD|Net).*$', '', created_by, flags=re.IGNORECASE)
            invoice['created_by'] = created_by.strip()

        # Extract payment terms
        if re.search(r'COD\s*-', text):
            invoice['payment_terms'] = 'COD'
        elif re.search(r'Net\s*\d+', text):
            net_match = re.search(r'Net\s*(\d+)', text)
            if net_match:
                invoice['payment_terms'] = f"Net {net_match.group(1)}"
        elif re.search(r'Net.*-', text):
            # Fallback: "Net" followed by any characters and dash (numbers may be null bytes or spaces)
            # Default to Net 30 as it's the most common term
            invoice['payment_terms'] = 'Net 30'

        # Use PyMuPDF-specific vendor extraction if distributor not found
        # PyMuPDF extracts text line-by-line which is cleaner for vendor extraction
        if not invoice['distributor']:
            pymupdf_vendor = self._extract_vendor_from_header(text)
            if pymupdf_vendor:
                invoice['distributor'] = pymupdf_vendor

        # Set legacy fields for backward compatibility
        invoice['vendor'] = invoice['distributor']
        invoice['vendor_address'] = invoice['distributor_address']
        invoice['vendor_license'] = invoice['distributor_license']
        invoice['customer_name'] = invoice['receiver']
        invoice['customer_address'] = invoice['receiver_address']
        invoice['status'] = invoice['invoice_status']

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

        # Extract line items: use pre-extracted if available, otherwise use tables/text
        if preextracted_line_items:
            invoice['line_items'] = preextracted_line_items
        else:
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
            if not table or len(table) < 1:
                continue

            # Check if this looks like an invoice items table by examining the first row
            # pdfplumber tables may not have headers - data rows start immediately
            first_row = table[0] if table else []

            # Check if first row has enough columns and looks like data (not just 2-3 columns of UI elements)
            # Line item tables typically have 10-11 columns
            if len(first_row) >= 10:
                # Try to parse this table - _parse_table_rows will validate rows
                items_from_table = self._parse_table_rows(table)
                if items_from_table:
                    line_items.extend(items_from_table)

        # If we got items from tables, return them
        if line_items:
            return line_items

        # Otherwise fall back to text parsing
        return self._extract_line_items_from_text(text)

    def _extract_line_items_from_pymupdf_text(self, text: str) -> List[Dict]:
        """
        FAST: Extract line items directly from PyMuPDF text output.
        PyMuPDF extracts text line-by-line, so we parse the multi-line structure.

        This is ~300ms faster than pdfplumber table extraction.

        Expected format (each line item spans multiple lines):
        1
        BRAND_NAME
        PRODUCT NAME [SIZE]
        TYPE - SUBTYPE
        TRACE_ID SKU_INFO
        UNITS
        $UNIT_COST
        $EXCISE
        $TOTAL
        $TOTAL_W_EXCISE
        """
        line_items = []
        lines = text.split('\n')

        # Find where line items start (after header, which contains "Item #" or "Item" + "#")
        start_idx = -1
        for i, line in enumerate(lines):
            line_stripped = line.strip()
            # Check for "Item #" on one line or "Item" followed by "#" on next line
            if 'Item #' in line:
                start_idx = i + 1
                break
            elif line_stripped == 'Item' and i + 1 < len(lines) and lines[i + 1].strip() == '#':
                start_idx = i + 2  # Skip both "Item" and "#"
                break

        if start_idx < 0:
            return []

        # Skip header lines (Brand, Product, Type - Subtype, etc.)
        header_keywords = {
            'Brand', 'Product', 'Type - Subtype', 'Trace Treez ID', 'SKU',
            'Units Cost', 'Excise / unit Total Cost Total Cost w/ Excise',
            'Excise /', 'unit', 'Total', 'Cost', 'Total Cost w/', 'Excise', ''
        }
        while start_idx < len(lines) and lines[start_idx].strip() in header_keywords:
            start_idx += 1

        # Parse line items - each item consists of:
        # item_num, brand, product, type-subtype, trace_id [sku], units, cost, excise, total, total_w_excise
        i = start_idx
        expected_item_num = 1  # Track expected item number for better detection
        while i < len(lines):
            line = lines[i].strip()

            # Stop at totals section
            if self._RE_TOTALS.match(line):
                break

            # Skip empty lines and page breaks
            if not line or 'Print Window' in line or 'Need Help' in line or line == 'menu':
                i += 1
                continue

            # Line item starts with the expected item number (1, 2, 3, ...)
            # This distinguishes from units values like "18", "24", etc.
            if self._RE_ITEM_NUM.match(line):
                num = int(line)
                # Must be the expected item number (allow some tolerance for skipped items)
                if num != expected_item_num and (num < expected_item_num or num > expected_item_num + 3):
                    i += 1
                    continue

                item_num = num
                expected_item_num = item_num + 1

                # Collect the next lines for this item
                # We need: brand, product, type, trace_id, units, cost, excise, total, total_w_excise
                item_lines = []
                i += 1
                found_type_line = False
                found_trace_id = False

                while i < len(lines) and len(item_lines) < 15:
                    next_line = lines[i].strip()

                    # Stop conditions
                    if not next_line or 'Print Window' in next_line or 'Need Help' in next_line:
                        i += 1
                        continue
                    if self._RE_TOTALS.match(next_line):
                        break

                    # Track if we've seen the TYPE - line and trace_id
                    if self._RE_TYPE_KEYWORD.match(next_line):
                        found_type_line = True
                    if found_type_line and self._RE_TRACE_ID.match(next_line):
                        found_trace_id = True

                    # Next item number signals end of current item
                    # Only check after we have TYPE and trace_id, and have prices
                    if self._RE_ITEM_NUM.match(next_line):
                        next_num = int(next_line)
                        # If this looks like the next item number and we have enough data
                        if next_num == expected_item_num and found_type_line and found_trace_id:
                            break

                    item_lines.append(next_line)
                    i += 1

                # Parse collected lines
                if len(item_lines) >= 6:  # Minimum needed: brand, product, type, trace_id, units, prices
                    try:
                        # Find the TYPE - line to anchor our parsing
                        # TYPE is one of: CARTRIDGE, EDIBLE, PREROLL, FLOWER, etc.
                        type_line_idx = -1
                        for idx, item_line in enumerate(item_lines):
                            if self._RE_TYPE_KEYWORD.match(item_line.strip()):
                                type_line_idx = idx
                                break

                        if type_line_idx < 0:
                            i += 1
                            continue

                        # Everything before TYPE is brand + product (could be 1-4 lines)
                        # Everything after TYPE is trace_id, units, prices
                        brand_product_lines = item_lines[:type_line_idx]
                        type_line = self._clean_text(item_lines[type_line_idx])
                        trace_line_idx = type_line_idx + 1

                        # Parse brand and product from collected lines
                        # Common patterns:
                        # 1. Single line each: "HIMALAYA", "GORILLA GLUE #4 1G"
                        # 2. Brand split: "ABSOLUTE", "XTRACTS", "25MG SOFT GELS"
                        # 3. Merged: "HIMALAYA SOUR BANANA SHERBET 1G"
                        if len(brand_product_lines) == 1:
                            # Merged brand + product
                            merged = self._clean_text(brand_product_lines[0])
                            parts = merged.split(' ', 1)
                            brand = parts[0]
                            product = parts[1] if len(parts) > 1 else merged
                        elif len(brand_product_lines) == 2:
                            brand = self._clean_text(brand_product_lines[0])
                            product = self._clean_text(brand_product_lines[1])
                        elif len(brand_product_lines) >= 3:
                            # Multi-line brand (e.g., "ABSOLUTE" + "XTRACTS") and product
                            # Heuristic: brand is first 1-2 short uppercase words
                            brand_parts = []
                            product_start = 0
                            for idx, bp_line in enumerate(brand_product_lines):
                                bp_clean = self._clean_text(bp_line)
                                # Brand lines are typically short and all caps
                                if len(bp_clean) <= 15 and bp_clean.isupper() and not re.search(r'\d', bp_clean):
                                    brand_parts.append(bp_clean)
                                    product_start = idx + 1
                                else:
                                    break
                            brand = ' '.join(brand_parts) if brand_parts else self._clean_text(brand_product_lines[0])
                            product = ' '.join(self._clean_text(l) for l in brand_product_lines[product_start:])
                        else:
                            brand = 'UNKNOWN'
                            product = 'UNKNOWN'

                        # Type - Subtype (may be just "TYPE -" or "TYPE - SUBTYPE")
                        type_match = re.match(r'([A-Z]+)\s*-\s*(.*)?', type_line, re.IGNORECASE)
                        if type_match:
                            prod_type = type_match.group(1).upper()
                            subtype = type_match.group(2).strip() if type_match.group(2) else ''
                        else:
                            prod_type = type_line.upper() if type_line else 'UNKNOWN'
                            subtype = ''

                        # Trace ID (with optional SKU suffix)
                        trace_line = self._clean_text(item_lines[trace_line_idx]) if trace_line_idx < len(item_lines) else ''
                        trace_match = re.match(r'([A-Z0-9]+)', trace_line)
                        trace_id = trace_match.group(1) if trace_match else trace_line

                        # Parse numeric fields (units, costs)
                        # Find the lines with pricing data
                        units = 0
                        unit_cost = 0.0
                        excise = 0.0
                        total_cost = 0.0
                        total_w_excise = 0.0

                        # Start looking for numeric data after trace_id
                        numeric_start_idx = trace_line_idx + 1
                        for j, val in enumerate(item_lines[numeric_start_idx:]):
                            val = val.strip()
                            # Units (first plain number)
                            if self._RE_ITEM_NUM.match(val) and units == 0:
                                units = int(val)
                            # Cost values (with $ sign)
                            elif val.startswith('$') or self._RE_PRICE.match(val):
                                amount = float(val.replace('$', '').replace(',', ''))
                                if unit_cost == 0:
                                    unit_cost = amount
                                elif excise == 0:
                                    excise = amount
                                elif total_cost == 0:
                                    total_cost = amount
                                elif total_w_excise == 0:
                                    total_w_excise = amount
                                    break

                        # Validate we got reasonable data
                        if units > 0 and trace_id:
                            # Extract size from product
                            size_match = self._RE_SIZE.search(product)
                            unit_size = size_match.group(1).strip() if size_match else None

                            line_item = {
                                'line_number': item_num,
                                'brand': brand,
                                'product_name': product,
                                'product_type': prod_type,
                                'product_subtype': subtype,
                                'strain': None,
                                'unit_size': unit_size,
                                'trace_id': trace_id,
                                'sku_units': units,
                                'unit_cost': unit_cost,
                                'excise_per_unit': excise,
                                'total_cost': total_cost,
                                'total_cost_with_excise': total_w_excise if total_w_excise else total_cost,
                                'is_promo': '[PROMO]' in product
                            }
                            line_items.append(line_item)
                    except Exception:
                        pass  # Skip malformed items
            else:
                i += 1

        return line_items

    def _is_non_treez_format(self, text: str) -> bool:
        """
        Detect non-Treez invoice formats that cannot be parsed by this parser.
        Returns True if the invoice format is not a standard Treez invoice.
        """
        # Inventory receiving system format has different header structure
        # Pattern: "Created Date: ... Accepted Date: ... Distributor: ... INVOICE #"
        # vs Treez: "VENDOR_NAME Barbary Coast Dispensary FULFILLED INVOICE#"
        inventory_format_patterns = [
            r'Created\s+Date:.*Accepted\s+Date:.*Distributor:',  # Inventory format header
            r'INVOICE\s*#\s*\d+\s*\n.*Created\s+Date:',  # Invoice # on separate line before dates
            r'Inventory\s+Location:',  # Inventory location field
            r'View\s+In\s+Inventory\s+Make\s+Payment',  # UI buttons from inventory system
            r'Items\s*\(\d+\)\s*\n.*Line\s+Item\s+Color\s+Codes:',  # Items count with color codes
        ]

        for pattern in inventory_format_patterns:
            if re.search(pattern, text, re.IGNORECASE | re.DOTALL):
                return True

        # Also check for absence of Treez format markers
        # Treez invoices always have: "VENDOR Barbary Coast Dispensary STATUS INVOICE#"
        has_treez_header = re.search(
            r'(Barbary\s+Coast\s+Dispensary|Grass\s+Roots)\s+(FULFILLED|PENDING|CANCELLED)\s+INVOICE#',
            text,
            re.IGNORECASE
        )

        # If no Treez header found but has INVOICE #, it's likely a different format
        has_invoice_marker = re.search(r'INVOICE\s*#', text, re.IGNORECASE)
        if has_invoice_marker and not has_treez_header:
            # Check if it looks like any known vendor format
            if not re.search(r'Print\s+Window', text):
                return True

        return False

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
                # Extended product types to cover all cannabis and non-cannabis products
                type_match = re.match(r'(PREROLL|BEVERA\s*GE|EXTRACT|FLOWER|CARTRIDGE|EDIBLE|VAPE|TINCTURE|MERCH|MERCHANDISE|ACCESSORY|TOPICAL|CAPSULE|CONCENTRATE|SUPPLY|SUPPLIES|ROLLING|INFUSED|APPAREL|GEAR|DEVICE|HARDWARE|PILL|TABLET|SUBLINGUAL|OIL|SPRAY|PATCH|BALM|LOTION|CREAM)\s*-\s*(.+)', type_cell, re.IGNORECASE | re.DOTALL)
                if type_match:
                    prod_type = type_match.group(1).upper().replace(' ', '')
                    subtype = self._clean_text(type_match.group(2))
                else:
                    # Try to extract just the type without subtype
                    type_only_match = re.match(r'(PREROLL|BEVERA\s*GE|EXTRACT|FLOWER|CARTRIDGE|EDIBLE|VAPE|TINCTURE|MERCH|MERCHANDISE|ACCESSORY|TOPICAL|CAPSULE|CONCENTRATE|SUPPLY|SUPPLIES|ROLLING|INFUSED|APPAREL|GEAR|DEVICE|HARDWARE|PILL|TABLET|SUBLINGUAL|OIL|SPRAY|PATCH|BALM|LOTION|CREAM)', type_cell, re.IGNORECASE)
                    if type_only_match:
                        prod_type = type_only_match.group(1).upper().replace(' ', '')
                        subtype = type_cell[type_only_match.end():].strip(' -')
                    else:
                        prod_type = 'UNKNOWN'
                        subtype = type_cell if type_cell else 'UNKNOWN'

                # Column 4: Trace ID
                # 1A prefix is for cannabis items tracked by Metrc (Barbary Coast)
                # 7D prefix is used by Grass Roots (Treez internal ID format)
                # Non-cannabis products (MERCH, ACCESSORY, ROLLING PAPERS, etc.) may have different IDs or N/A
                trace_id = self._clean_text(row[offset + 4] or '')

                # Validate trace ID based on product type
                is_non_cannabis = prod_type in ('MERCH', 'MERCHANDISE', 'ACCESSORY', 'ROLLING', 'SUPPLY', 'SUPPLIES', 'APPAREL', 'GEAR', 'DEVICE', 'HARDWARE')

                if not trace_id:
                    continue

                # For cannabis products, accept 1A (Metrc) or 7D (Treez internal) prefixes
                # For non-cannabis, accept any non-empty trace ID
                if not is_non_cannabis and not re.match(r'(1A|7D)', trace_id):
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
            # Extended product types to cover cannabis and non-cannabis products
            pattern = r'^(\d+)\s+\((\d+)\)\s+(.+?)\s+(PREROLL|BEVERA\s*GE|EXTRACT|FLOWER|CARTRIDGE|EDIBLE|VAPE|TINCTURE|MERCH|MERCHANDISE|ACCESSORY|TOPICAL|CAPSULE|CONCENTRATE|SUPPLY|SUPPLIES|ROLLING|INFUSED|APPAREL|GEAR|DEVICE|HARDWARE|PILL|TABLET|SUBLINGUAL|OIL|SPRAY|PATCH|BALM|LOTION|CREAM)\s+-\s+([A-Z\s]+?)\s+([A-Z0-9]+)\s+(\d+)\s+\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})'

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
                # Extended product types and flexible trace ID (not just 1A prefix)
                pricing_match = re.search(
                    r'(PREROLL|BEVERA\s*GE|EXTRACT|FLOWER|CARTRIDGE|EDIBLE|VAPE|TINCTURE|MERCH|MERCHANDISE|ACCESSORY|TOPICAL|CAPSULE|CONCENTRATE|SUPPLY|SUPPLIES|ROLLING|INFUSED|APPAREL|GEAR|DEVICE|HARDWARE|PILL|TABLET|SUBLINGUAL|OIL|SPRAY|PATCH|BALM|LOTION|CREAM).*?'  # Product type (anywhere in combined text)
                    r'([A-Z0-9]{10,})\s+'  # Trace ID (alphanumeric, at least 10 chars)
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
            subtype_match = re.search(r'(?:PREROLL|BEVERA\s*GE|EXTRACT|FLOWER|CARTRIDGE|EDIBLE|VAPE|TINCTURE|MERCH|MERCHANDISE|ACCESSORY|TOPICAL|CAPSULE|CONCENTRATE|SUPPLY|SUPPLIES|ROLLING|INFUSED|APPAREL|GEAR|DEVICE|HARDWARE|PILL|TABLET|SUBLINGUAL|OIL|SPRAY|PATCH|BALM|LOTION|CREAM)\s*-\s*([A-Z\s]+?)(?:[A-Z0-9]{10,}|$)', combined_text)
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
            text_clean = re.sub(r'(PREROLL|BEVERA\s*GE|EXTRACT|FLOWER|CARTRIDGE|EDIBLE|VAPE|TINCTURE|MERCH|MERCHANDISE|ACCESSORY|TOPICAL|CAPSULE|CONCENTRATE|SUPPLY|SUPPLIES|ROLLING|INFUSED|APPAREL|GEAR|DEVICE|HARDWARE|PILL|TABLET|SUBLINGUAL|OIL|SPRAY|PATCH|BALM|LOTION|CREAM)\s*-?\s*$', '', text_clean, flags=re.IGNORECASE).strip()

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

        # Initialize boto3 client with timeout configuration
        from botocore.config import Config
        boto_config = Config(
            connect_timeout=5,
            read_timeout=10,
            retries={'max_attempts': 2}
        )

        session_kwargs = {'region_name': region, 'config': boto_config}
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

            # Prepare invoice header - remove None values for DynamoDB
            invoice_header = {
                'invoice_id': invoice_data.get('invoice_id') or invoice_data.get('invoice_number') or 'UNKNOWN',
                'invoice_number': invoice_data.get('invoice_number') or 'UNKNOWN',
                'line_item_count': len(invoice_data.get('line_items', []))
            }

            # Add optional fields only if they have non-None values
            # Note: invoice_date is the "Created:" date from the PDF header (when business created the invoice)
            # download_date is when the invoice was downloaded/exported (from filename timestamp)
            optional_fields = {
                'vendor': invoice_data.get('vendor'),
                'vendor_license': invoice_data.get('vendor_license'),
                'vendor_address': invoice_data.get('vendor_address'),
                'customer_name': invoice_data.get('customer_name'),
                'customer_address': invoice_data.get('customer_address'),
                'invoice_date': invoice_data.get('invoice_date'),
                'download_date': invoice_data.get('download_date'),
                'created_by': invoice_data.get('created_by'),
                'payment_terms': invoice_data.get('payment_terms'),
                'status': invoice_data.get('status'),
                'source_file': invoice_data.get('source_file'),
                'extracted_at': invoice_data.get('extracted_at')
            }

            for key, value in optional_fields.items():
                if value is not None:
                    invoice_header[key] = value

            # Add numeric fields (DynamoDB needs Decimal type)
            invoice_header['subtotal'] = Decimal(str(invoice_data.get('invoice_subtotal', 0)))
            invoice_header['discount'] = Decimal(str(invoice_data.get('invoice_discount', 0)))
            invoice_header['fees'] = Decimal(str(invoice_data.get('invoice_fees', 0)))
            invoice_header['tax'] = Decimal(str(invoice_data.get('invoice_tax', 0)))
            invoice_header['total'] = Decimal(str(invoice_data.get('invoice_total', 0)))
            invoice_header['balance'] = Decimal(str(invoice_data.get('balance', 0)))

            # Store invoice header
            invoices_table.put_item(Item=invoice_header)

            # Store line items
            invoice_id = invoice_header['invoice_id']
            for item in invoice_data.get('line_items', []):
                # Required fields
                line_item = {
                    'invoice_id': invoice_id,
                    'line_number': item['line_number'],
                    'brand': item.get('brand') or 'UNKNOWN',
                    'product_name': item.get('product_name') or 'UNKNOWN',
                    'product_type': item.get('product_type') or 'UNKNOWN',
                    'product_subtype': item.get('product_subtype') or 'UNKNOWN',
                    'trace_id': item.get('trace_id') or 'UNKNOWN',
                    'sku_units': item.get('sku_units', 0),
                    'unit_cost': Decimal(str(item.get('unit_cost', 0))),
                    'excise_per_unit': Decimal(str(item.get('excise_per_unit', 0))),
                    'total_cost': Decimal(str(item.get('total_cost', 0))),
                    'total_cost_with_excise': Decimal(str(item.get('total_cost_with_excise', 0))),
                    'is_promo': item.get('is_promo', False)
                }

                # Add optional fields only if not None
                if item.get('strain'):
                    line_item['strain'] = item['strain']
                if item.get('unit_size'):
                    line_item['unit_size'] = item['unit_size']
                # invoice_date = when business created the invoice (from PDF "Created:" header)
                if invoice_data.get('invoice_date'):
                    line_item['invoice_date'] = invoice_data['invoice_date']
                # download_date = when invoice was downloaded/exported (from filename timestamp)
                if invoice_data.get('download_date'):
                    line_item['download_date'] = invoice_data['download_date']

                line_items_table.put_item(Item=line_item)

            return True

        except Exception as e:
            import traceback
            error_msg = f"Error storing invoice: {e}\n{traceback.format_exc()}"
            print(error_msg)
            # Also store the error message so it can be accessed by the UI
            if hasattr(self, 'last_error'):
                self.last_error = error_msg
            else:
                self.last_error = error_msg
            return False

    def get_invoice_summary(self, start_date: str = None, end_date: str = None) -> Dict:
        """
        Get aggregated invoice summary for Claude analysis.
        Returns only the essential data needed for analysis.
        """
        invoices_table = self.dynamodb.Table(self.invoices_table_name)

        # Always use scan - it's more reliable and works without requiring specific indexes
        response = invoices_table.scan()
        invoices = response.get('Items', [])

        # Handle pagination
        while 'LastEvaluatedKey' in response:
            response = invoices_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            invoices.extend(response.get('Items', []))

        # Filter by date if specified (post-scan filtering)
        if start_date or end_date:
            filtered = []
            for inv in invoices:
                inv_date = inv.get('invoice_date')
                if inv_date:
                    if start_date and inv_date < start_date:
                        continue
                    if end_date and inv_date > end_date:
                        continue
                filtered.append(inv)
            invoices = filtered

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

        # Handle pagination
        while 'LastEvaluatedKey' in response:
            response = line_items_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response.get('Items', []))

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
