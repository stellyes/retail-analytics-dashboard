"""
QR Code Integration Module
Refactored QR Code Generator with Click Tracking for Retail Analytics Dashboard
"""

import streamlit as st
import qrcode
from qrcode.image.styledpil import StyledPilImage
from qrcode.image.styles.moduledrawers import RoundedModuleDrawer
from qrcode.image.styles.colormasks import SolidFillColorMask
from PIL import Image, ImageDraw
import io
import base64
import boto3
from boto3.dynamodb.conditions import Key, Attr
import pandas as pd
from datetime import datetime, timedelta
import json
import uuid


# =============================================================================
# QR CODE GENERATION FUNCTIONS
# =============================================================================

def generate_short_code():
    """Generate a unique short code for the QR code."""
    return uuid.uuid4().hex[:8]


def create_qr_code(url: str, logo_image=None, box_size: int = 10,
                   border: int = 4, fill_color: str = "#000000",
                   back_color: str = "#FFFFFF", error_correction: str = "H",
                   logo_size_ratio: float = 0.25):
    """
    Generate a QR code with rounded edges and optional logo.
    """
    # Map error correction levels
    error_levels = {
        "L": qrcode.constants.ERROR_CORRECT_L,
        "M": qrcode.constants.ERROR_CORRECT_M,
        "Q": qrcode.constants.ERROR_CORRECT_Q,
        "H": qrcode.constants.ERROR_CORRECT_H
    }

    # Create QR code instance
    qr = qrcode.QRCode(
        version=None,  # Auto-determine
        error_correction=error_levels.get(error_correction, qrcode.constants.ERROR_CORRECT_H),
        box_size=box_size,
        border=border
    )

    qr.add_data(url)
    qr.make(fit=True)

    # Convert hex colors to RGB
    def hex_to_rgb(hex_color):
        hex_color = hex_color.lstrip('#')
        return tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))

    fill_rgb = hex_to_rgb(fill_color)
    back_rgb = hex_to_rgb(back_color)

    # If logo is provided, create with cleared center area
    if logo_image is not None:
        img = create_qr_with_clear_center(qr, fill_rgb, back_rgb, logo_image, logo_size_ratio)
    else:
        # Create styled image with rounded modules (no logo)
        img = qr.make_image(
            image_factory=StyledPilImage,
            module_drawer=RoundedModuleDrawer(),
            color_mask=SolidFillColorMask(
                back_color=back_rgb,
                front_color=fill_rgb
            )
        )

        # Convert to PIL Image if needed
        if not isinstance(img, Image.Image):
            img = img.get_image()

    return img


def create_qr_with_clear_center(qr, fill_rgb, back_rgb, logo_img, logo_size_ratio: float = 0.25):
    """Create a QR code with a cleared center area for the logo."""
    # Create styled image with rounded modules
    img = qr.make_image(
        image_factory=StyledPilImage,
        module_drawer=RoundedModuleDrawer(),
        color_mask=SolidFillColorMask(
            back_color=back_rgb,
            front_color=fill_rgb
        )
    )

    # Convert to PIL Image if needed
    if not isinstance(img, Image.Image):
        img = img.get_image()

    img = img.convert("RGBA")
    logo_img = logo_img.convert("RGBA")

    # Calculate logo dimensions
    qr_width, qr_height = img.size
    logo_max_size = int(min(qr_width, qr_height) * logo_size_ratio)

    # Resize logo maintaining aspect ratio
    logo_img.thumbnail((logo_max_size, logo_max_size), Image.Resampling.LANCZOS)
    logo_width, logo_height = logo_img.size

    # Calculate padding and background size
    padding = 20
    bg_size = (logo_width + padding * 2, logo_height + padding * 2)

    # Make the cleared area even larger
    clear_margin = 15
    clear_size = (bg_size[0] + clear_margin * 2, bg_size[1] + clear_margin * 2)

    # Calculate center positions
    clear_pos = (
        (qr_width - clear_size[0]) // 2,
        (qr_height - clear_size[1]) // 2
    )

    bg_pos = (
        (qr_width - bg_size[0]) // 2,
        (qr_height - bg_size[1]) // 2
    )

    # Clear the center area
    draw = ImageDraw.Draw(img)
    draw.rectangle(
        [clear_pos, (clear_pos[0] + clear_size[0], clear_pos[1] + clear_size[1])],
        fill=back_rgb + (255,)
    )

    # Create white background
    bg = Image.new("RGBA", bg_size, back_rgb + (255,))

    # Calculate logo position
    logo_pos = (
        bg_pos[0] + padding,
        bg_pos[1] + padding
    )

    # Paste background and logo
    img.paste(bg, bg_pos, bg)
    img.paste(logo_img, logo_pos, logo_img)

    return img


# =============================================================================
# DATABASE FUNCTIONS
# =============================================================================

def get_dynamodb_client():
    """Initialize DynamoDB client with credentials from secrets."""
    try:
        # Try different secret configurations
        if hasattr(st, 'secrets'):
            if 'qr_aws' in st.secrets:
                # QR-specific AWS config
                return boto3.resource(
                    'dynamodb',
                    region_name=st.secrets['qr_aws'].get("region", "us-east-1"),
                    aws_access_key_id=st.secrets['qr_aws'].get("access_key_id"),
                    aws_secret_access_key=st.secrets['qr_aws'].get("secret_access_key")
                )
            elif 'aws' in st.secrets:
                # Use retail AWS config
                return boto3.resource(
                    'dynamodb',
                    region_name=st.secrets['aws'].get("region", "us-east-1"),
                    aws_access_key_id=st.secrets['aws'].get("access_key_id"),
                    aws_secret_access_key=st.secrets['aws'].get("secret_access_key")
                )
            elif st.secrets.get("aws_region"):
                # Legacy format
                return boto3.resource(
                    'dynamodb',
                    region_name=st.secrets.get("aws_region", "us-east-1"),
                    aws_access_key_id=st.secrets.get("aws_access_key_id"),
                    aws_secret_access_key=st.secrets.get("aws_secret_access_key")
                )
    except Exception as e:
        st.warning(f"AWS DynamoDB not configured for QR tracking: {e}")
        return None

    return None


def get_qr_codes_table(dynamodb):
    """Get the QR codes table."""
    if dynamodb:
        try:
            table_name = st.secrets.get("dynamodb_qr_table", "qr_codes")
            if 'qr_aws' in st.secrets:
                table_name = st.secrets['qr_aws'].get("qr_table", "qr_codes")
            return dynamodb.Table(table_name)
        except:
            return None
    return None


def get_clicks_table(dynamodb):
    """Get the clicks table."""
    if dynamodb:
        try:
            table_name = st.secrets.get("dynamodb_clicks_table", "qr_clicks")
            if 'qr_aws' in st.secrets:
                table_name = st.secrets['qr_aws'].get("clicks_table", "qr_clicks")
            return dynamodb.Table(table_name)
        except:
            return None
    return None


def save_qr_to_database(dynamodb, short_code: str, original_url: str,
                        name: str = "", description: str = ""):
    """Save QR code metadata to DynamoDB."""
    table = get_qr_codes_table(dynamodb)
    if table:
        try:
            table.put_item(Item={
                'short_code': short_code,
                'original_url': original_url,
                'name': name,
                'description': description,
                'created_at': datetime.utcnow().isoformat(),
                'total_clicks': 0,
                'active': True
            })
            return True
        except Exception as e:
            st.error(f"Error saving to database: {e}")
            return False
    return False


def get_all_qr_codes(dynamodb, include_deleted: bool = False):
    """Retrieve all QR codes from database."""
    table = get_qr_codes_table(dynamodb)
    if table:
        try:
            if include_deleted:
                response = table.scan()
            else:
                response = table.scan(
                    FilterExpression=Attr('deleted').not_exists() | Attr('deleted').eq(False)
                )
            return response.get('Items', [])
        except Exception as e:
            st.error(f"Error fetching QR codes: {e}")
            return []
    return []


def get_click_analytics(dynamodb, short_code: str = None, days: int = 30):
    """Retrieve click analytics from database."""
    table = get_clicks_table(dynamodb)
    if table:
        try:
            cutoff_date = (datetime.utcnow() - timedelta(days=days)).isoformat()

            if short_code:
                response = table.query(
                    KeyConditionExpression=Key('short_code').eq(short_code),
                    FilterExpression=Attr('timestamp').gte(cutoff_date)
                )
            else:
                response = table.scan(
                    FilterExpression=Attr('timestamp').gte(cutoff_date)
                )

            return response.get('Items', [])
        except Exception as e:
            st.error(f"Error fetching analytics: {e}")
            return []
    return []


def delete_qr_code(dynamodb, short_code: str):
    """Soft delete a QR code from the database."""
    table = get_qr_codes_table(dynamodb)
    if table:
        try:
            table.update_item(
                Key={'short_code': short_code},
                UpdateExpression='SET deleted = :deleted, deleted_at = :timestamp, active = :active',
                ExpressionAttributeValues={
                    ':deleted': True,
                    ':timestamp': datetime.utcnow().isoformat(),
                    ':active': False
                }
            )
            return True
        except Exception as e:
            st.error(f"Error deleting QR code: {e}")
            return False
    return False


def restore_qr_code(dynamodb, short_code: str):
    """Restore a deleted QR code."""
    table = get_qr_codes_table(dynamodb)
    if table:
        try:
            table.update_item(
                Key={'short_code': short_code},
                UpdateExpression='REMOVE deleted, deleted_at SET active = :active',
                ExpressionAttributeValues={
                    ':active': True
                }
            )
            return True
        except Exception as e:
            st.error(f"Error restoring QR code: {e}")
            return False
    return False


def get_recently_deleted_qr_codes(dynamodb, minutes: int = 5):
    """Get QR codes deleted within the last N minutes."""
    table = get_qr_codes_table(dynamodb)
    if table:
        try:
            cutoff_time = (datetime.utcnow() - timedelta(minutes=minutes)).isoformat()
            response = table.scan(
                FilterExpression=Attr('deleted').eq(True) & Attr('deleted_at').gte(cutoff_time)
            )
            return response.get('Items', [])
        except Exception as e:
            st.error(f"Error fetching deleted QR codes: {e}")
            return []
    return []


def permanently_delete_old_qr_codes(dynamodb, minutes: int = 5):
    """Permanently delete QR codes that were marked as deleted more than N minutes ago."""
    table = get_qr_codes_table(dynamodb)
    if table:
        try:
            cutoff_time = (datetime.utcnow() - timedelta(minutes=minutes)).isoformat()
            response = table.scan(
                FilterExpression=Attr('deleted').eq(True) & Attr('deleted_at').lt(cutoff_time)
            )
            old_deleted = response.get('Items', [])

            # Permanently delete old items
            for item in old_deleted:
                table.delete_item(Key={'short_code': item['short_code']})

            return len(old_deleted)
        except Exception as e:
            st.error(f"Error cleaning up deleted QR codes: {e}")
            return 0
    return 0


# =============================================================================
# PAGE RENDERING FUNCTIONS (FOR TABS)
# =============================================================================

def render_qr_page():
    """Main QR Code page with tabs for all QR functionality."""
    st.header("📱 QR Code Generator & Tracker")

    # Initialize AWS connection
    dynamodb = get_dynamodb_client()

    # Get redirect base URL from secrets
    redirect_base_url = st.secrets.get("redirect_base_url", "https://your-api-gateway-url.execute-api.region.amazonaws.com/prod/r")
    if 'qr_aws' in st.secrets:
        redirect_base_url = st.secrets['qr_aws'].get("redirect_base_url", redirect_base_url)

    # Create tabs for different QR functions
    tab1, tab2, tab3, tab4 = st.tabs(["🎨 Generate QR Code", "📊 Analytics Dashboard", "📋 Manage QR Codes", "⚙️ Settings"])

    with tab1:
        generate_qr_tab(dynamodb, redirect_base_url)

    with tab2:
        analytics_tab(dynamodb)

    with tab3:
        manage_qr_tab(dynamodb, redirect_base_url)

    with tab4:
        settings_tab()


def generate_qr_tab(dynamodb, redirect_base_url):
    """QR Code generation tab."""

    st.subheader("Generate New QR Code")

    col1, col2 = st.columns([1, 1])

    with col1:
        st.markdown("#### QR Code Settings")

        # Basic settings
        qr_name = st.text_input("QR Code Name", placeholder="My Marketing Campaign")
        qr_description = st.text_area("Description (optional)", placeholder="Describe the purpose of this QR code")
        target_url = st.text_input("Target URL", placeholder="https://example.com/landing-page")

        st.divider()

        # Styling options
        st.markdown("#### Styling Options")

        col_a, col_b = st.columns(2)
        with col_a:
            fill_color = st.color_picker("QR Code Color", "#000000")
            box_size = st.slider("Box Size", 5, 20, 10)
        with col_b:
            back_color = st.color_picker("Background Color", "#FFFFFF")
            border = st.slider("Border Size", 1, 10, 4)

        error_correction = st.selectbox(
            "Error Correction Level",
            ["H (High - 30%)", "Q (Quartile - 25%)", "M (Medium - 15%)", "L (Low - 7%)"],
            help="Higher error correction allows for logo overlay but creates denser QR codes"
        )
        error_level = error_correction.split(" ")[0]

        st.divider()

        # Logo upload
        st.markdown("#### Logo (Optional)")
        logo_file = st.file_uploader(
            "Upload logo for center of QR code",
            type=['png', 'jpg', 'jpeg', 'gif'],
            help="Recommended: Square image, PNG with transparent background"
        )

        logo_size_ratio = st.slider(
            "Logo Size (%)",
            10, 35, 25,
            help="Logo size as percentage of QR code. Larger logos need higher error correction."
        ) / 100

        # Generate button
        generate_clicked = st.button("🎨 Generate QR Code", type="primary", use_container_width=True)

    with col2:
        st.markdown("#### Preview")

        if generate_clicked and target_url:
            with st.spinner("Generating QR code..."):
                # Generate short code for tracking
                short_code = generate_short_code()

                # Create tracking URL
                tracking_url = f"{redirect_base_url}/{short_code}"

                # Process logo if uploaded
                logo_image = None
                if logo_file:
                    logo_image = Image.open(logo_file)

                # Generate QR code
                qr_img = create_qr_code(
                    url=tracking_url,
                    logo_image=logo_image,
                    box_size=box_size,
                    border=border,
                    fill_color=fill_color,
                    back_color=back_color,
                    error_correction=error_level,
                    logo_size_ratio=logo_size_ratio
                )

                # Store in session state
                st.session_state['generated_qr'] = qr_img
                st.session_state['qr_data'] = {
                    'short_code': short_code,
                    'original_url': target_url,
                    'tracking_url': tracking_url,
                    'name': qr_name,
                    'description': qr_description
                }

        # Display generated QR code
        if 'generated_qr' in st.session_state:
            qr_img = st.session_state['generated_qr']
            qr_data = st.session_state['qr_data']

            # Display QR code
            st.image(qr_img, use_container_width=True)

            # Display tracking info
            st.info(f"**Tracking URL:** `{qr_data['tracking_url']}`")
            st.caption(f"**Redirects to:** {qr_data['original_url']}")

            # Download buttons
            col_dl1, col_dl2 = st.columns(2)

            # PNG download
            with col_dl1:
                png_buffer = io.BytesIO()
                qr_img.save(png_buffer, format='PNG')
                png_buffer.seek(0)
                st.download_button(
                    label="📥 Download PNG",
                    data=png_buffer.getvalue(),
                    file_name=f"qr_{qr_data['short_code']}.png",
                    mime="image/png",
                    use_container_width=True
                )

            # High-res download
            with col_dl2:
                hr_buffer = io.BytesIO()
                hr_img = qr_img.resize((1000, 1000), Image.Resampling.LANCZOS)
                hr_img.save(hr_buffer, format='PNG')
                hr_buffer.seek(0)
                st.download_button(
                    label="📥 Download Hi-Res",
                    data=hr_buffer.getvalue(),
                    file_name=f"qr_{qr_data['short_code']}_hires.png",
                    mime="image/png",
                    use_container_width=True
                )

            # Save to database button
            if dynamodb:
                if st.button("💾 Save to Database", type="secondary", use_container_width=True):
                    if save_qr_to_database(
                        dynamodb,
                        qr_data['short_code'],
                        qr_data['original_url'],
                        qr_data['name'],
                        qr_data['description']
                    ):
                        st.success("✅ QR code saved to database!")
                    else:
                        st.error("Failed to save to database")
            else:
                st.warning("⚠️ AWS DynamoDB not configured. QR code tracking will not work.")
        else:
            st.info("Enter a URL and click 'Generate QR Code' to create your QR code")


def analytics_tab(dynamodb):
    """Analytics dashboard tab."""

    st.subheader("📊 Analytics Dashboard")

    if not dynamodb:
        st.warning("⚠️ AWS DynamoDB not configured. Analytics unavailable.")
        st.info("Please configure AWS credentials in your Streamlit secrets to enable analytics.")
        return

    # Date range selector
    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        days = st.selectbox("Time Period", [7, 14, 30, 60, 90], index=2)
    with col2:
        qr_codes = get_all_qr_codes(dynamodb)
        qr_options = ["All QR Codes"] + [f"{qr.get('name', 'Unnamed')} ({qr['short_code']})" for qr in qr_codes]
        selected_qr = st.selectbox("Filter by QR Code", qr_options)

    # Get selected short_code
    selected_short_code = None
    if selected_qr != "All QR Codes":
        selected_short_code = selected_qr.split("(")[-1].rstrip(")")

    # Fetch analytics data
    clicks = get_click_analytics(dynamodb, selected_short_code, days)

    if not clicks:
        st.info("No click data available for the selected period.")
        return

    # Convert to DataFrame
    df = pd.DataFrame(clicks)

    # Overview metrics
    st.markdown("#### Overview")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Clicks", len(df))

    with col2:
        unique_visitors = df['ip_address'].nunique() if 'ip_address' in df.columns else 0
        st.metric("Unique Visitors", unique_visitors)

    with col3:
        unique_qrs = df['short_code'].nunique() if 'short_code' in df.columns else 0
        st.metric("Active QR Codes", unique_qrs)

    with col4:
        if 'timestamp' in df.columns:
            df['date'] = pd.to_datetime(df['timestamp']).dt.date
            avg_daily = len(df) / df['date'].nunique() if df['date'].nunique() > 0 else 0
            st.metric("Avg. Daily Clicks", f"{avg_daily:.1f}")

    st.divider()

    # Export buttons
    col_export1, col_export2 = st.columns([1, 1])

    with col_export1:
        # CSV Export
        csv_buffer = io.BytesIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        st.download_button(
            label="📥 Download CSV",
            data=csv_buffer.getvalue(),
            file_name=f"qr_analytics_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            use_container_width=True
        )

    with col_export2:
        # Excel Export
        try:
            excel_buffer = io.BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Click Data', index=False)

            excel_buffer.seek(0)

            st.download_button(
                label="📥 Download Excel",
                data=excel_buffer.getvalue(),
                file_name=f"qr_analytics_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
        except ImportError:
            st.info("Excel export requires 'openpyxl' package")

    st.divider()

    # Charts
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Clicks Over Time")
        if 'timestamp' in df.columns:
            df['date'] = pd.to_datetime(df['timestamp']).dt.date
            daily_clicks = df.groupby('date').size().reset_index(name='clicks')
            st.line_chart(daily_clicks.set_index('date'))

    with col2:
        st.markdown("#### Top QR Codes")
        if 'short_code' in df.columns:
            top_qrs = df['short_code'].value_counts().head(10)
            st.bar_chart(top_qrs)

    st.divider()

    # Recent clicks table
    st.markdown("#### Recent Clicks")
    display_cols = ['timestamp', 'short_code', 'country', 'city', 'device_type', 'browser']
    available_cols = [col for col in display_cols if col in df.columns]
    if available_cols:
        st.dataframe(
            df[available_cols].sort_values('timestamp', ascending=False).head(50),
            use_container_width=True
        )


def manage_qr_tab(dynamodb, redirect_base_url):
    """QR Code management tab."""

    st.subheader("📋 Manage QR Codes")

    if not dynamodb:
        st.warning("⚠️ AWS DynamoDB not configured. Management unavailable.")
        return

    # Clean up old deleted QR codes
    permanently_delete_old_qr_codes(dynamodb, minutes=5)

    # Get recently deleted QR codes
    recently_deleted = get_recently_deleted_qr_codes(dynamodb, minutes=5)

    # Show recently deleted section if there are any
    if recently_deleted:
        st.markdown("#### 🕐 Recently Deleted (Undo Available)")
        st.info(f"You have {len(recently_deleted)} recently deleted QR code(s). You can restore them within 5 minutes of deletion.")

        for qr in recently_deleted:
            deleted_time = datetime.fromisoformat(qr.get('deleted_at', ''))
            time_remaining = timedelta(minutes=5) - (datetime.utcnow() - deleted_time)
            minutes_left = int(time_remaining.total_seconds() / 60)
            seconds_left = int(time_remaining.total_seconds() % 60)

            with st.expander(f"🗑️ **{qr.get('name', 'Unnamed')}** - {qr['short_code']}", expanded=False):
                col1, col2 = st.columns([2, 1])

                with col1:
                    st.write(f"**Short Code:** `{qr['short_code']}`")
                    st.write(f"**Target URL:** {qr.get('original_url', 'N/A')}")
                    st.write(f"**Total Clicks:** {qr.get('total_clicks', 0)}")
                    if qr.get('description'):
                        st.write(f"**Description:** {qr['description']}")
                    st.warning(f"⏰ Permanent deletion in {minutes_left}m {seconds_left}s")

                with col2:
                    if st.button("↩️ Undo Delete", key=f"restore_{qr['short_code']}", type="primary", use_container_width=True):
                        if restore_qr_code(dynamodb, qr['short_code']):
                            st.success(f"✅ QR code {qr['short_code']} restored successfully!")
                            st.rerun()
                        else:
                            st.error("Failed to restore QR code")

        st.divider()

    # Get active QR codes
    qr_codes = get_all_qr_codes(dynamodb)

    if not qr_codes:
        st.info("No QR codes found. Generate your first QR code!")
        return

    st.markdown("#### Active QR Codes")

    # Sort by created date
    qr_codes.sort(key=lambda x: x.get('created_at', ''), reverse=True)

    for qr in qr_codes:
        with st.expander(f"**{qr.get('name', 'Unnamed')}** - {qr['short_code']}", expanded=False):
            col1, col2 = st.columns([2, 1])

            with col1:
                st.write(f"**Short Code:** `{qr['short_code']}`")
                st.write(f"**Target URL:** {qr.get('original_url', 'N/A')}")
                st.write(f"**Tracking URL:** `{redirect_base_url}/{qr['short_code']}`")
                st.write(f"**Created:** {qr.get('created_at', 'Unknown')}")
                st.write(f"**Total Clicks:** {qr.get('total_clicks', 0)}")
                if qr.get('description'):
                    st.write(f"**Description:** {qr['description']}")

            with col2:
                # Regenerate QR code for display
                qr_img = create_qr_code(
                    url=f"{redirect_base_url}/{qr['short_code']}",
                    box_size=6,
                    border=2
                )
                st.image(qr_img, width=150)

                # Download button
                png_buffer = io.BytesIO()
                qr_img.save(png_buffer, format='PNG')
                png_buffer.seek(0)
                st.download_button(
                    label="📥 Download",
                    data=png_buffer.getvalue(),
                    file_name=f"qr_{qr['short_code']}.png",
                    mime="image/png",
                    key=f"dl_{qr['short_code']}"
                )

                # Delete button
                if st.button("🗑️ Delete", key=f"delete_{qr['short_code']}", type="secondary", use_container_width=True):
                    st.session_state[f'confirm_delete_{qr["short_code"]}'] = True
                    st.rerun()

                # Show confirmation dialog if delete was clicked
                if st.session_state.get(f'confirm_delete_{qr["short_code"]}', False):
                    st.warning("⚠️ Are you sure?")
                    col_a, col_b = st.columns(2)
                    with col_a:
                        if st.button("Yes", key=f"confirm_yes_{qr['short_code']}", type="primary", use_container_width=True):
                            if delete_qr_code(dynamodb, qr['short_code']):
                                st.success(f"✅ Deleted {qr['short_code']}!")
                                del st.session_state[f'confirm_delete_{qr["short_code"]}']
                                st.rerun()
                            else:
                                st.error("Failed to delete QR code")
                    with col_b:
                        if st.button("Cancel", key=f"confirm_no_{qr['short_code']}", use_container_width=True):
                            del st.session_state[f'confirm_delete_{qr["short_code"]}']
                            st.rerun()


def settings_tab():
    """Settings tab."""

    st.subheader("⚙️ Settings")

    st.markdown("#### AWS Configuration Status")

    # Check AWS configuration
    aws_configured = False
    if hasattr(st, 'secrets'):
        if 'qr_aws' in st.secrets:
            aws_configured = all([
                st.secrets['qr_aws'].get("access_key_id"),
                st.secrets['qr_aws'].get("secret_access_key"),
                st.secrets['qr_aws'].get("region")
            ])
        elif 'aws' in st.secrets:
            aws_configured = all([
                st.secrets['aws'].get("access_key_id"),
                st.secrets['aws'].get("secret_access_key"),
                st.secrets['aws'].get("region")
            ])

    if aws_configured:
        st.success("✅ AWS DynamoDB credentials configured")
        if 'qr_aws' in st.secrets:
            st.write(f"**Region:** {st.secrets['qr_aws'].get('region')}")
            st.write(f"**QR Codes Table:** {st.secrets['qr_aws'].get('qr_table', 'qr_codes')}")
            st.write(f"**Clicks Table:** {st.secrets['qr_aws'].get('clicks_table', 'qr_clicks')}")
        elif 'aws' in st.secrets:
            st.write(f"**Region:** {st.secrets['aws'].get('region')}")
    else:
        st.warning("⚠️ AWS DynamoDB not configured")
        st.info("""
        To configure AWS for QR tracking, add to your `.streamlit/secrets.toml`:
        ```toml
        [qr_aws]
        access_key_id = "your-access-key"
        secret_access_key = "your-secret-key"
        region = "us-east-1"
        qr_table = "qr_codes"
        clicks_table = "qr_clicks"
        redirect_base_url = "https://your-api-gateway-url/prod/r"
        ```
        """)

    st.divider()

    st.markdown("#### Redirect URL Configuration")
    redirect_url = st.secrets.get("redirect_base_url", "Not configured")
    if 'qr_aws' in st.secrets:
        redirect_url = st.secrets['qr_aws'].get("redirect_base_url", redirect_url)
    st.write(f"**Current Redirect Base URL:** `{redirect_url}`")

    st.divider()

    st.markdown("#### About QR Code Generator")
    st.write("""
    Features:
    - Generate QR codes with rounded edges
    - Add custom logos to QR codes
    - Track clicks and visitor data
    - View analytics dashboard
    - Manage all QR codes

    Integrated into Retail Analytics Dashboard for seamless marketing campaign tracking.
    """)
