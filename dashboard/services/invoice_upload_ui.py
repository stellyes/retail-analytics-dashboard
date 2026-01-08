"""
Invoice Upload UI Component
Provides a Streamlit interface for uploading invoice PDFs,
extracting data, and storing in DynamoDB.

Usage in app.py:
    from invoice_upload_ui import render_invoice_upload_section

    render_invoice_upload_section()
"""

import streamlit as st
from typing import List, Dict
import json
from datetime import datetime, date

try:
    from .invoice_extraction import TreezInvoiceParser, InvoiceDataService
    INVOICE_EXTRACTION_AVAILABLE = True
except ImportError:
    INVOICE_EXTRACTION_AVAILABLE = False


def _init_date_review_state():
    """Initialize session state for date review tracking."""
    if 'invoices_needing_date_review' not in st.session_state:
        st.session_state.invoices_needing_date_review = []
    if 'date_review_loaded_from_dynamo' not in st.session_state:
        st.session_state.date_review_loaded_from_dynamo = False
    if 'duplicate_invoices' not in st.session_state:
        st.session_state.duplicate_invoices = []


def _load_invoices_needing_review_from_dynamo():
    """
    Load invoices that need date review from DynamoDB.
    Finds invoices where invoice_date is missing or null.
    Called once on app startup.
    """
    if st.session_state.date_review_loaded_from_dynamo:
        return  # Already loaded

    try:
        aws_config = {
            'aws_access_key': st.secrets['aws']['access_key_id'],
            'aws_secret_key': st.secrets['aws']['secret_access_key'],
            'region': st.secrets['aws']['region']
        }
        invoice_service = InvoiceDataService(**aws_config)

        # Scan invoices table for items without invoice_date
        invoices_table = invoice_service.dynamodb.Table(invoice_service.invoices_table_name)

        # Scan all invoices and filter for those missing invoice_date
        response = invoices_table.scan()
        items = response.get('Items', [])

        # Handle pagination
        while 'LastEvaluatedKey' in response:
            response = invoices_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response.get('Items', []))

        # Find invoices needing date review (no invoice_date field)
        # Also detect duplicates based on invoice_number
        invoice_numbers_seen = {}

        for item in items:
            invoice_number = item.get('invoice_number')
            invoice_id = item.get('invoice_id')

            # Track for duplicate detection
            if invoice_number:
                if invoice_number not in invoice_numbers_seen:
                    invoice_numbers_seen[invoice_number] = []
                invoice_numbers_seen[invoice_number].append(item)

            # Add to date review if missing date
            if not item.get('invoice_date'):
                # Check if already in the review list
                existing_ids = [inv['invoice_id'] for inv in st.session_state.invoices_needing_date_review]
                if invoice_id and invoice_id not in existing_ids:
                    st.session_state.invoices_needing_date_review.append({
                        'invoice_id': invoice_id,
                        'invoice_number': invoice_number or invoice_id,
                        'filename': item.get('source_file', 'Unknown'),
                        'vendor': item.get('vendor', 'Unknown'),
                        'download_date': item.get('download_date'),
                        'total': float(item.get('total', 0)),
                        'added_at': datetime.now().isoformat()
                    })

        # Find duplicates (same invoice_number AND store appearing multiple times)
        # Group by invoice_number + store combination
        invoice_store_seen = {}
        for inv_num, invoices in invoice_numbers_seen.items():
            for inv in invoices:
                # Get store name from receiver or customer_name field
                store_name = inv.get('receiver') or inv.get('customer_name') or 'Unknown'
                # Normalize store name for comparison
                if 'barbary' in store_name.lower():
                    store_key = 'Barbary Coast'
                elif 'grass' in store_name.lower():
                    store_key = 'Grass Roots'
                else:
                    store_key = store_name

                composite_key = f"{inv_num}|{store_key}"
                if composite_key not in invoice_store_seen:
                    invoice_store_seen[composite_key] = []
                invoice_store_seen[composite_key].append(inv)

        # Now find actual duplicates (same invoice_number + store combination appearing multiple times)
        for composite_key, invoices in invoice_store_seen.items():
            if len(invoices) > 1:
                inv_num, store_name = composite_key.split('|', 1)
                # Sort by extracted_at to find oldest and newest
                sorted_invoices = sorted(invoices, key=lambda x: x.get('extracted_at', ''))
                st.session_state.duplicate_invoices.append({
                    'invoice_number': inv_num,
                    'store_name': store_name,
                    'count': len(invoices),
                    'invoices': [
                        {
                            'invoice_id': inv.get('invoice_id'),
                            'vendor': inv.get('vendor'),
                            'store_name': store_name,
                            'total': float(inv.get('total', 0)),
                            'invoice_date': inv.get('invoice_date'),
                            'download_date': inv.get('download_date'),
                            'extracted_at': inv.get('extracted_at'),
                            'source_file': inv.get('source_file')
                        }
                        for inv in sorted_invoices
                    ]
                })

        st.session_state.date_review_loaded_from_dynamo = True

    except Exception as e:
        # Silently fail - don't block app startup if DynamoDB isn't accessible
        st.session_state.date_review_loaded_from_dynamo = True  # Mark as attempted


def render_invoice_upload_section():
    """
    Render the invoice upload UI section.
    Handles PDF upload, extraction, and DynamoDB storage.
    """
    if not INVOICE_EXTRACTION_AVAILABLE:
        st.error("Invoice extraction module not available. Check dependencies.")
        return

    # Get AWS credentials from secrets
    try:
        aws_config = {
            'aws_access_key': st.secrets['aws']['access_key_id'],
            'aws_secret_key': st.secrets['aws']['secret_access_key'],
            'region': st.secrets['aws']['region']
        }
        aws_configured = True
    except Exception as e:
        st.error("⚠️ AWS credentials not found in secrets. Configure secrets to use invoice upload.")
        aws_configured = False
        return

    # Initialize services
    try:
        parser = TreezInvoiceParser()
        invoice_service = InvoiceDataService(**aws_config)

        # Check if tables exist on first load
        if 'invoice_tables_created' not in st.session_state:
            # Try to check if tables exist by describing them
            try:
                invoice_service.dynamodb.Table(invoice_service.invoices_table_name).table_status
                invoice_service.dynamodb.Table(invoice_service.line_items_table_name).table_status
                st.session_state.invoice_tables_created = True
            except Exception:
                st.session_state.invoice_tables_created = False

        # Only show setup if tables don't exist
        if not st.session_state.invoice_tables_created:
            with st.expander("⚙️ One-Time Setup: Create DynamoDB Tables", expanded=False):
                st.markdown("""
                Click the button below to create the required DynamoDB tables.
                This only needs to be done once.

                **Tables to be created:**
                - `retail-invoices` - Invoice headers
                - `retail-invoice-line-items` - Product line items
                """)

                if st.button("Create DynamoDB Tables"):
                    with st.spinner("Creating tables..."):
                        try:
                            invoice_service.create_tables()
                            st.session_state.invoice_tables_created = True
                            st.success("✓ Tables created successfully!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error creating tables: {str(e)}")
                            st.info("Tables may already exist. If so, you can proceed with uploading.")

    except Exception as e:
        st.error(f"Error initializing invoice services: {str(e)}")
        return

    st.markdown("---")

    # File uploader
    uploaded_files = st.file_uploader(
        "Upload Invoice PDFs",
        type=['pdf'],
        accept_multiple_files=True,
        help="Select one or more Treez invoice PDFs to process"
    )

    if uploaded_files:
        st.info(f"📄 {len(uploaded_files)} file(s) uploaded")

        # Process button
        if st.button("🚀 Process Invoices", type="primary", width='stretch'):
            process_invoices(uploaded_files, parser, invoice_service)

    # Show recent uploads if any
    if 'recent_invoice_uploads' in st.session_state and st.session_state.recent_invoice_uploads:
        st.markdown("---")
        st.subheader("Recent Uploads")
        display_recent_uploads(st.session_state.recent_invoice_uploads)


def process_invoices(uploaded_files: List, parser: TreezInvoiceParser,
                     invoice_service: InvoiceDataService):
    """
    Process uploaded invoice PDFs: Extract → Store in DynamoDB.

    Args:
        uploaded_files: List of uploaded file objects from Streamlit
        parser: TreezInvoiceParser instance
        invoice_service: InvoiceDataService instance
    """
    results = {
        'successful': [],
        'failed': [],
        'total': len(uploaded_files)
    }

    # Progress tracking
    progress_bar = st.progress(0)
    status_text = st.empty()

    # Results container
    results_container = st.container()

    for idx, uploaded_file in enumerate(uploaded_files):
        filename = uploaded_file.name
        status_text.text(f"Processing {idx + 1}/{len(uploaded_files)}: {filename}")

        try:
            # Read PDF bytes
            pdf_bytes = uploaded_file.read()

            # Save temporarily to process (pdfplumber needs a file path)
            # IMPORTANT: Preserve original filename for invoice number/date extraction fallback
            import tempfile
            import os

            # Create temp directory and save with original filename
            tmp_dir = tempfile.mkdtemp()
            tmp_path = os.path.join(tmp_dir, filename)
            with open(tmp_path, 'wb') as tmp_file:
                tmp_file.write(pdf_bytes)

            try:
                # Extract invoice data
                invoice_data = parser.extract_from_pdf(tmp_path)

                # Check for extraction errors
                if 'error' in invoice_data:
                    results['failed'].append({
                        'filename': filename,
                        'error': invoice_data['error'],
                        'stage': 'extraction'
                    })
                else:
                    # Check for duplicates before storing
                    # Duplicates are defined as same invoice_number + same store
                    invoice_number = invoice_data.get('invoice_number')
                    store_name = invoice_data.get('receiver') or invoice_data.get('customer_name') or ''
                    is_duplicate = False

                    # Normalize store name for comparison
                    if 'barbary' in store_name.lower():
                        store_key = 'Barbary Coast'
                    elif 'grass' in store_name.lower():
                        store_key = 'Grass Roots'
                    else:
                        store_key = store_name

                    if invoice_number:
                        try:
                            # Query DynamoDB to check if invoice already exists for THIS store
                            invoices_table = invoice_service.dynamodb.Table(invoice_service.invoices_table_name)
                            response = invoices_table.scan(
                                FilterExpression='invoice_number = :inv_num',
                                ExpressionAttributeValues={':inv_num': invoice_number}
                            )
                            # Check if any matching invoice is for the same store
                            if response.get('Items'):
                                for existing_inv in response['Items']:
                                    existing_store = existing_inv.get('receiver') or existing_inv.get('customer_name') or ''
                                    # Normalize existing store name
                                    if 'barbary' in existing_store.lower():
                                        existing_store_key = 'Barbary Coast'
                                    elif 'grass' in existing_store.lower():
                                        existing_store_key = 'Grass Roots'
                                    else:
                                        existing_store_key = existing_store

                                    # Only mark as duplicate if same store
                                    if existing_store_key == store_key:
                                        is_duplicate = True
                                        break
                        except:
                            pass  # If check fails, proceed with storing

                    # Store in DynamoDB
                    success = invoice_service.store_invoice(invoice_data)

                    if success:
                        result_item = {
                            'filename': filename,
                            'invoice_number': invoice_data.get('invoice_number'),
                            'invoice_id': invoice_data.get('invoice_id') or invoice_data.get('invoice_number'),
                            'vendor': invoice_data.get('vendor'),
                            'store_name': store_key,  # Include store for display
                            'total': invoice_data.get('invoice_total', 0),
                            'line_items': len(invoice_data.get('line_items', [])),
                            'invoice_date': invoice_data.get('invoice_date'),
                            'download_date': invoice_data.get('download_date'),
                            'date_extraction_failed': invoice_data.get('_date_extraction_failed', False),
                            'is_duplicate': is_duplicate
                        }
                        results['successful'].append(result_item)

                        # Track invoices needing date review
                        if invoice_data.get('_date_extraction_failed'):
                            _init_date_review_state()
                            # Add to review list if not already there
                            existing_ids = [inv['invoice_id'] for inv in st.session_state.invoices_needing_date_review]
                            if result_item['invoice_id'] not in existing_ids:
                                st.session_state.invoices_needing_date_review.append({
                                    'invoice_id': result_item['invoice_id'],
                                    'invoice_number': result_item['invoice_number'],
                                    'filename': filename,
                                    'vendor': result_item['vendor'],
                                    'download_date': result_item['download_date'],
                                    'total': result_item['total'],
                                    'added_at': datetime.now().isoformat()
                                })
                    else:
                        # Get detailed error if available
                        error_detail = getattr(invoice_service, 'last_error', 'Failed to store in DynamoDB')
                        results['failed'].append({
                            'filename': filename,
                            'error': error_detail,
                            'stage': 'storage'
                        })

            finally:
                # Clean up temp file and directory
                import shutil
                try:
                    os.unlink(tmp_path)
                    shutil.rmtree(tmp_dir, ignore_errors=True)
                except:
                    pass

        except Exception as e:
            results['failed'].append({
                'filename': filename,
                'error': str(e),
                'stage': 'processing'
            })

        # Update progress
        progress_bar.progress((idx + 1) / len(uploaded_files))

    # Clear progress indicators
    status_text.empty()
    progress_bar.empty()

    # Display results
    display_processing_results(results, results_container)

    # Store in session state for "Recent Uploads" section
    if 'recent_invoice_uploads' not in st.session_state:
        st.session_state.recent_invoice_uploads = []

    st.session_state.recent_invoice_uploads.insert(0, {
        'timestamp': datetime.now().isoformat(),
        'results': results
    })

    # Keep only last 5 upload batches
    st.session_state.recent_invoice_uploads = st.session_state.recent_invoice_uploads[:5]


def display_processing_results(results: Dict, container):
    """Display the results of invoice processing."""
    with container:
        st.markdown("---")
        st.subheader("Processing Results")

        # Summary metrics
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Total Uploaded", results['total'])
        with col2:
            st.metric("✓ Successful", len(results['successful']))
        with col3:
            st.metric("✗ Failed", len(results['failed']))

        # Successful uploads
        if results['successful']:
            st.success(f"✓ Successfully processed {len(results['successful'])} invoice(s)")

            # Check for invoices needing date review
            needs_review = [item for item in results['successful'] if item.get('date_extraction_failed')]
            if needs_review:
                st.warning(f"⚠️ {len(needs_review)} invoice(s) need manual date entry. Go to the '📅 Date Review' tab.")

            # Check for duplicates
            duplicates = [item for item in results['successful'] if item.get('is_duplicate')]
            if duplicates:
                st.warning(f"⚠️ {len(duplicates)} duplicate invoice(s) detected. Go to the '🔍 Duplicates' tab to review.")

            with st.expander("View Successful Uploads", expanded=True):
                for item in results['successful']:
                    date_display = item['invoice_date'] if item['invoice_date'] else "⚠️ Needs manual entry"
                    date_warning = " *(date extraction failed)*" if item.get('date_extraction_failed') else ""
                    duplicate_warning = " **⚠️ DUPLICATE**" if item.get('is_duplicate') else ""
                    store_display = item.get('store_name', 'Unknown')
                    st.markdown(f"""
                    **{item['filename']}**{duplicate_warning}
                    - Invoice #: `{item['invoice_number']}`
                    - Store: {store_display}
                    - Vendor: {item['vendor']}
                    - Date: {date_display}{date_warning}
                    - Total: ${item['total']:,.2f}
                    - Line Items: {item['line_items']}
                    """)
                    st.markdown("---")

        # Failed uploads
        if results['failed']:
            st.error(f"✗ Failed to process {len(results['failed'])} invoice(s)")

            with st.expander("View Errors", expanded=True):
                for item in results['failed']:
                    st.markdown(f"""
                    **{item['filename']}**
                    - Stage: {item['stage']}
                    - Error: `{item['error']}`
                    """)
                    st.markdown("---")

        # Success message
        if results['successful'] and not results['failed']:
            st.balloons()
            st.success("🎉 All invoices processed successfully! You can now use Claude analytics to analyze your data.")


def display_recent_uploads(recent_uploads: List[Dict]):
    """Display history of recent upload batches."""
    for upload_batch in recent_uploads:
        timestamp = datetime.fromisoformat(upload_batch['timestamp'])
        results = upload_batch['results']

        with st.expander(
            f"📅 {timestamp.strftime('%Y-%m-%d %H:%M:%S')} - "
            f"{len(results['successful'])} successful, {len(results['failed'])} failed"
        ):
            if results['successful']:
                st.markdown("**✓ Successful:**")
                for item in results['successful']:
                    st.markdown(f"- {item['filename']} (Invoice #{item['invoice_number']})")

            if results['failed']:
                st.markdown("**✗ Failed:**")
                for item in results['failed']:
                    st.markdown(f"- {item['filename']}: {item['error']}")


def render_invoice_data_viewer():
    """
    Optional: Render a data viewer for stored invoices.
    Shows summary of what's in DynamoDB.
    """
    st.header("📊 Stored Invoice Data")

    try:
        aws_config = {
            'aws_access_key': st.secrets['aws']['access_key_id'],
            'aws_secret_key': st.secrets['aws']['secret_access_key'],
            'region': st.secrets['aws']['region']
        }

        invoice_service = InvoiceDataService(**aws_config)

        # Date range filter
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Start Date", value=None)
        with col2:
            end_date = st.date_input("End Date", value=None)

        start_str = start_date.strftime('%Y-%m-%d') if start_date else None
        end_str = end_date.strftime('%Y-%m-%d') if end_date else None

        if st.button("Load Invoice Summary"):
            with st.spinner("Loading data..."):
                # Get invoice summary
                invoice_summary = invoice_service.get_invoice_summary(start_str, end_str)
                product_summary = invoice_service.get_product_summary(start_str, end_str)

                # Display summary metrics
                st.subheader("Summary")

                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Invoices", invoice_summary['total_invoices'])
                with col2:
                    st.metric("Total Value", f"${invoice_summary['total_value']:,.2f}")
                with col3:
                    st.metric("Avg Invoice", f"${invoice_summary['avg_invoice_value']:,.2f}")
                with col4:
                    st.metric("Line Items", invoice_summary['total_line_items'])

                # Vendor breakdown
                if invoice_summary['vendors']:
                    st.subheader("Vendors")
                    vendors_data = []
                    for vendor, data in invoice_summary['vendors'].items():
                        vendors_data.append({
                            'Vendor': vendor,
                            'Invoices': data['count'],
                            'Total Spend': f"${data['total']:,.2f}"
                        })
                    st.table(vendors_data)

                # Top brands
                if product_summary['brands']:
                    st.subheader("Top Brands")
                    top_brands = sorted(
                        product_summary['brands'].items(),
                        key=lambda x: x[1]['total_cost'],
                        reverse=True
                    )[:10]

                    brands_data = []
                    for brand, data in top_brands:
                        brands_data.append({
                            'Brand': brand,
                            'Products': data['product_count'],
                            'Units': data['total_units'],
                            'Total Cost': f"${data['total_cost']:,.2f}"
                        })
                    st.table(brands_data)

                # Product types
                if product_summary['product_types']:
                    st.subheader("Product Types")
                    types_data = []
                    for ptype, data in product_summary['product_types'].items():
                        types_data.append({
                            'Type': ptype,
                            'Products': data['product_count'],
                            'Units': data['total_units'],
                            'Total Cost': f"${data['total_cost']:,.2f}"
                        })
                    st.table(types_data)

    except Exception as e:
        st.error(f"Error loading invoice data: {str(e)}")


# Convenience function for app.py integration
def render_full_invoice_section():
    """
    Render complete invoice management section with upload and viewer.
    Use this in app.py for a complete invoice management interface.
    """
    _init_date_review_state()

    # Load invoices needing review from DynamoDB on first run
    _load_invoices_needing_review_from_dynamo()

    tab1, tab2, tab3, tab4 = st.tabs(["📤 Upload Invoices", "📊 View Data", "📅 Date Review", "🔍 Duplicates"])

    with tab1:
        render_invoice_upload_section()

    with tab2:
        render_invoice_data_viewer()

    with tab3:
        render_date_review_section()

    with tab4:
        render_duplicates_section()


def render_date_review_section():
    """
    Render the date review section for invoices with failed date extraction.
    Allows manual entry of invoice dates that couldn't be automatically extracted.
    """
    st.header("📅 Invoice Date Review")

    _init_date_review_state()

    invoices_to_review = st.session_state.invoices_needing_date_review

    if not invoices_to_review:
        st.success("✅ No invoices need date review!")
        st.info("When you upload invoices with dates that can't be automatically extracted, they'll appear here for manual entry.")
        return

    st.markdown(f"""
    **{len(invoices_to_review)} invoice(s)** need manual date entry.

    These invoices have PDF rendering issues that prevent automatic date extraction.
    Please enter the "Created" date from each invoice's header.
    """)

    # Get AWS credentials and service
    try:
        aws_config = {
            'aws_access_key': st.secrets['aws']['access_key_id'],
            'aws_secret_key': st.secrets['aws']['secret_access_key'],
            'region': st.secrets['aws']['region']
        }
        invoice_service = InvoiceDataService(**aws_config)
    except Exception as e:
        st.error(f"⚠️ AWS credentials not found. Cannot update invoices: {e}")
        return

    # Stats
    st.info(f"📊 **{len(invoices_to_review)}** invoices pending date review")

    st.markdown("---")

    # Process each invoice
    for idx, invoice in enumerate(invoices_to_review):
        col1, col2, col3, col4 = st.columns([2, 2, 2, 1])

        with col1:
            st.markdown(f"**Invoice #{invoice['invoice_number']}**")
            st.caption(f"Vendor: {invoice.get('vendor', 'Unknown')}")
            st.caption(f"File: {invoice.get('filename', 'Unknown')}")

        with col2:
            st.caption(f"Download Date: {invoice.get('download_date', 'Unknown')}")
            st.caption(f"Total: ${invoice.get('total', 0):,.2f}")

        with col3:
            # Date input for manual entry
            # Try to parse download_date as a default starting point
            default_date = None
            if invoice.get('download_date'):
                try:
                    parts = invoice['download_date'].split('-')
                    default_date = date(int(parts[0]), int(parts[1]), int(parts[2]))
                except:
                    pass

            selected_date = st.date_input(
                "Created Date",
                value=default_date,
                key=f"date_review_{invoice['invoice_id']}",
                help="Enter the 'Created:' date from the PDF header"
            )

        with col4:
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("💾 Save", key=f"save_date_{invoice['invoice_id']}", width='stretch'):
                if selected_date:
                    # Update the invoice in DynamoDB
                    date_str = selected_date.strftime('%Y-%m-%d')
                    success = _update_invoice_date(
                        invoice_service,
                        invoice['invoice_id'],
                        date_str
                    )

                    if success:
                        st.success(f"✅ Updated invoice #{invoice['invoice_number']} with date {date_str}")
                        # Remove from review list
                        st.session_state.invoices_needing_date_review = [
                            inv for inv in st.session_state.invoices_needing_date_review
                            if inv['invoice_id'] != invoice['invoice_id']
                        ]
                        st.rerun()
                    else:
                        st.error("Failed to update invoice date")
                else:
                    st.warning("Please select a date")

        st.markdown("---")

    # Bulk actions
    st.subheader("Bulk Actions")
    col1, col2 = st.columns(2)

    with col1:
        if st.button("🗑️ Clear All Pending Reviews", width='stretch'):
            st.session_state.invoices_needing_date_review = []
            st.success("Cleared all pending reviews")
            st.rerun()

    with col2:
        st.caption("Use this to dismiss all pending reviews without updating dates")


def _update_invoice_date(invoice_service: InvoiceDataService, invoice_id: str, invoice_date: str) -> bool:
    """
    Update the invoice_date for an invoice in DynamoDB.

    Args:
        invoice_service: InvoiceDataService instance
        invoice_id: The invoice ID to update
        invoice_date: The new date string (YYYY-MM-DD format)

    Returns:
        True if successful, False otherwise
    """
    try:
        # Update invoice header
        invoices_table = invoice_service.dynamodb.Table(invoice_service.invoices_table_name)
        invoices_table.update_item(
            Key={'invoice_id': invoice_id},
            UpdateExpression='SET invoice_date = :date',
            ExpressionAttributeValues={':date': invoice_date}
        )

        # Update all line items for this invoice
        line_items_table = invoice_service.dynamodb.Table(invoice_service.line_items_table_name)

        # Query all line items for this invoice
        response = line_items_table.query(
            KeyConditionExpression='invoice_id = :inv_id',
            ExpressionAttributeValues={':inv_id': invoice_id}
        )

        # Update each line item
        for item in response.get('Items', []):
            line_items_table.update_item(
                Key={
                    'invoice_id': invoice_id,
                    'line_number': item['line_number']
                },
                UpdateExpression='SET invoice_date = :date',
                ExpressionAttributeValues={':date': invoice_date}
            )

        return True

    except Exception as e:
        print(f"Error updating invoice date: {e}")
        return False


def render_duplicates_section():
    """
    Render the duplicates section showing invoices with the same invoice_number.
    Allows users to review and delete duplicate entries.
    """
    st.header("🔍 Duplicate Invoices")

    _init_date_review_state()

    duplicates = st.session_state.duplicate_invoices

    if not duplicates:
        st.success("✅ No duplicate invoices found!")
        st.info("Duplicate detection looks for invoices with the same invoice number **and** store. Invoice #665 for Grass Roots and #665 for Barbary Coast are NOT duplicates.")
        return

    st.markdown(f"""
    **{len(duplicates)} invoice/store combination(s)** have duplicates in the database.

    Review each duplicate set below and delete unwanted copies.
    *Note: Only invoices with the same number AND same store are considered duplicates.*
    """)

    # Get AWS credentials and service
    try:
        aws_config = {
            'aws_access_key': st.secrets['aws']['access_key_id'],
            'aws_secret_key': st.secrets['aws']['secret_access_key'],
            'region': st.secrets['aws']['region']
        }
        invoice_service = InvoiceDataService(**aws_config)
    except Exception as e:
        st.error(f"⚠️ AWS credentials not found. Cannot manage duplicates: {e}")
        return

    # Stats
    total_duplicate_count = sum(dup['count'] for dup in duplicates)
    st.warning(f"📊 **{total_duplicate_count}** total invoice entries across **{len(duplicates)}** invoice numbers")

    st.markdown("---")

    # Process each duplicate set
    for dup_set in duplicates:
        store_label = dup_set.get('store_name', 'Unknown Store')
        with st.expander(f"Invoice #{dup_set['invoice_number']} ({store_label}) - {dup_set['count']} copies", expanded=False):
            st.markdown(f"**{dup_set['count']} entries found for invoice #{dup_set['invoice_number']} at {store_label}**")

            for idx, invoice in enumerate(dup_set['invoices']):
                col1, col2, col3 = st.columns([3, 2, 1])

                with col1:
                    st.markdown(f"**Copy {idx + 1}**")
                    st.caption(f"Invoice ID: {invoice['invoice_id']}")
                    st.caption(f"Store: {invoice.get('store_name', store_label)}")
                    st.caption(f"Vendor: {invoice.get('vendor', 'Unknown')}")
                    st.caption(f"File: {invoice.get('source_file', 'Unknown')}")

                with col2:
                    st.caption(f"Date: {invoice.get('invoice_date', 'None')}")
                    st.caption(f"Download: {invoice.get('download_date', 'Unknown')}")
                    st.caption(f"Total: ${invoice.get('total', 0):,.2f}")
                    st.caption(f"Extracted: {invoice.get('extracted_at', 'Unknown')[:10]}")

                with col3:
                    st.markdown("<br>", unsafe_allow_html=True)
                    if st.button("🗑️ Delete", key=f"delete_dup_{invoice['invoice_id']}", width='stretch'):
                        success = _delete_invoice(invoice_service, invoice['invoice_id'])
                        if success:
                            st.success(f"✅ Deleted invoice {invoice['invoice_id']}")
                            # Remove from duplicates list
                            # Reload duplicates from DynamoDB
                            st.session_state.date_review_loaded_from_dynamo = False
                            st.session_state.duplicate_invoices = []
                            st.rerun()
                        else:
                            st.error("Failed to delete invoice")

                st.markdown("---")

    # Bulk actions
    st.subheader("Bulk Actions")
    if st.button("🔄 Refresh Duplicate Detection", width='stretch'):
        st.session_state.date_review_loaded_from_dynamo = False
        st.session_state.duplicate_invoices = []
        st.rerun()


def _delete_invoice(invoice_service: InvoiceDataService, invoice_id: str) -> bool:
    """
    Delete an invoice and all its line items from DynamoDB.

    Args:
        invoice_service: InvoiceDataService instance
        invoice_id: The invoice ID to delete

    Returns:
        True if successful, False otherwise
    """
    try:
        # Delete invoice header
        invoices_table = invoice_service.dynamodb.Table(invoice_service.invoices_table_name)
        invoices_table.delete_item(Key={'invoice_id': invoice_id})

        # Delete all line items for this invoice
        line_items_table = invoice_service.dynamodb.Table(invoice_service.line_items_table_name)

        # Query all line items for this invoice
        response = line_items_table.query(
            KeyConditionExpression='invoice_id = :inv_id',
            ExpressionAttributeValues={':inv_id': invoice_id}
        )

        # Delete each line item
        for item in response.get('Items', []):
            line_items_table.delete_item(
                Key={
                    'invoice_id': invoice_id,
                    'line_number': item['line_number']
                }
            )

        return True

    except Exception as e:
        print(f"Error deleting invoice: {e}")
        return False
