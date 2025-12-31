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
from datetime import datetime

try:
    from invoice_extraction import TreezInvoiceParser, InvoiceDataService
    INVOICE_EXTRACTION_AVAILABLE = True
except ImportError:
    INVOICE_EXTRACTION_AVAILABLE = False


def render_invoice_upload_section():
    """
    Render the invoice upload UI section.
    Handles PDF upload, extraction, and DynamoDB storage.
    """
    if not INVOICE_EXTRACTION_AVAILABLE:
        st.error("Invoice extraction module not available. Check dependencies.")
        return

    st.header("📋 Invoice Data Upload")
    st.markdown("""
    Upload invoice PDFs to automatically extract data and store in DynamoDB.

    **Process:**
    1. Upload one or more PDF invoices
    2. Data is extracted from each PDF (no Claude API costs)
    3. Extracted data is stored in DynamoDB
    4. View extraction results and any errors
    """)

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

        # Check if tables exist, offer to create them
        if 'invoice_tables_created' not in st.session_state:
            st.session_state.invoice_tables_created = False

        if not st.session_state.invoice_tables_created:
            with st.expander("⚙️ One-Time Setup: Create DynamoDB Tables", expanded=False):
                st.markdown("""
                Click the button below to create the required DynamoDB tables.
                This only needs to be done once.

                **Tables to be created:**
                - `retail-invoices` - Invoice headers
                - `retail-invoice-line-items` - Product line items
                - `retail-invoice-aggregations` - Pre-computed summaries
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
        if st.button("🚀 Process Invoices", type="primary", use_container_width=True):
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

            # Save temporarily to process (PyPDF2 needs a file path)
            import tempfile
            import os

            with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp_file:
                tmp_file.write(pdf_bytes)
                tmp_path = tmp_file.name

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
                    # Store in DynamoDB
                    success = invoice_service.store_invoice(invoice_data)

                    if success:
                        results['successful'].append({
                            'filename': filename,
                            'invoice_number': invoice_data.get('invoice_number'),
                            'vendor': invoice_data.get('vendor'),
                            'total': invoice_data.get('invoice_total', 0),
                            'line_items': len(invoice_data.get('line_items', [])),
                            'invoice_date': invoice_data.get('invoice_date')
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
                # Clean up temp file
                os.unlink(tmp_path)

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

            with st.expander("View Successful Uploads", expanded=True):
                for item in results['successful']:
                    st.markdown(f"""
                    **{item['filename']}**
                    - Invoice #: `{item['invoice_number']}`
                    - Vendor: {item['vendor']}
                    - Date: {item['invoice_date']}
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
    tab1, tab2 = st.tabs(["📤 Upload Invoices", "📊 View Data"])

    with tab1:
        render_invoice_upload_section()

    with tab2:
        render_invoice_data_viewer()
