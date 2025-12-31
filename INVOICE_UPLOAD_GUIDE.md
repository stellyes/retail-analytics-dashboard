# Invoice Upload - Quick Start Guide

## Overview

The invoice analytics feature is now fully integrated into the **Data Upload** page. Upload PDFs and the system automatically:
- Extracts all invoice data (no Claude API costs!)
- Stores in DynamoDB for fast querying
- Makes data available for Claude-powered insights

## How to Use

### 1. Navigate to Data Upload
In your Streamlit app, click **📤 Data Upload** in the sidebar.

### 2. Find Invoice Upload Section
Scroll down to **📋 Invoice Data Upload**.

### 3. First-Time Setup (One-Time Only)
Click the **"Create DynamoDB Tables"** button in the setup expander. This creates:
- `retail-invoices` - Invoice headers
- `retail-invoice-line-items` - Line items
- `retail-invoice-aggregations` - Summaries

### 4. Upload Your Invoices
1. Click **"Upload Invoice PDFs"**
2. Select one or more Treez invoice PDF files
3. Click **"🚀 Process Invoices"**

Watch the magic happen:
```
Processing 1/5: invoice_13835_20251230_170518.pdf
✓ Invoice #: 13835
✓ Total: $2,136.76
✓ Items: 17
✓ Stored in DynamoDB

Processing 2/5: invoice_13830_20251230_170556.pdf
...
```

### 5. View Your Data
Switch to the **"📊 View Data"** tab to see:
- Total invoices and spend
- Vendor breakdown
- Top brands
- Product types

## Features

### Auto-Extraction
- Parses Treez invoice PDFs using regex patterns
- No Claude API calls = FREE extraction
- Processes ~1-2 seconds per invoice

### Progress Tracking
- Real-time progress bar
- Success/failure status for each invoice
- Error details if extraction fails

### Recent Uploads
- History of last 5 upload batches
- Quick summary of successes/failures
- Easy reference for troubleshooting

### Error Handling
- Shows which invoices failed and why
- Continues processing remaining invoices if one fails
- Detailed error messages for debugging

## Cost Savings

**Traditional Approach (Claude Vision API for extraction):**
- 100 invoices × $0.75 = $75 for extraction alone
- Plus $10-20 for analysis queries
- **Total: ~$85-95**

**This Implementation:**
- PDF extraction: $0 (regex parsing)
- DynamoDB: ~$3/month
- Claude analysis: $10-15/month
- **Total: ~$13-18/month**

**Savings: 80-85%**

## AWS Requirements

### IAM Permissions Needed
Your AWS user needs these DynamoDB permissions:

```json
{
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Action": [
            "dynamodb:CreateTable",
            "dynamodb:DescribeTable",
            "dynamodb:PutItem",
            "dynamodb:GetItem",
            "dynamodb:Scan",
            "dynamodb:Query"
        ],
        "Resource": "arn:aws:dynamodb:us-west-1:*:table/retail-*"
    }]
}
```

### Secrets Configuration
Your `.streamlit/secrets.toml` should include:

```toml
[aws]
access_key_id = "YOUR_AWS_KEY"
secret_access_key = "YOUR_AWS_SECRET"
region = "us-west-1"

[anthropic]
ANTHROPIC_API_KEY = "YOUR_CLAUDE_KEY"
```

## Supported Invoice Format

Currently optimized for **Treez** invoice PDFs with this structure:
- Vendor info (top left)
- Customer/dispensary info (center top)
- Invoice number, date, status
- Line item table with columns:
  - Item #, Brand, Product, Type-Subtype
  - Trace Treez ID, SKU, Units, Cost
  - Excise/unit, Total Cost
- Footer totals (Subtotal, Discounts, Fees, Tax, Total)

## Troubleshooting

### "AccessDeniedException" Error
- Check that AWS IAM policy is attached to your user
- Verify region matches (us-west-1)
- Ensure credentials in secrets.toml are correct

### No Data Extracted
- Invoice may not be in Treez format
- Check PDF is text-based (not scanned image)
- Review extraction patterns in `invoice_extraction.py`

### Table Creation Fails
- Tables may already exist (check DynamoDB console)
- Verify IAM permissions include `dynamodb:CreateTable`
- Check region matches secrets configuration

## Next Steps

Once invoices are uploaded:

1. **View Summaries** - Use the data viewer to explore your purchasing patterns
2. **Claude Analysis** - Coming soon: AI-powered insights on vendors, products, and pricing
3. **Regular Uploads** - Upload new invoices as they come in to keep data current
4. **Export Data** - Query DynamoDB directly for custom reports

## Technical Details

### Data Flow
```
PDF Upload
    ↓
PyPDF2 Text Extraction
    ↓
Regex Pattern Matching
    ↓
JSON Structure
    ↓
DynamoDB Storage (2 tables)
    ↓
Pre-Aggregation for Claude
    ↓
Claude Analysis (on-demand)
```

### Database Schema

**retail-invoices table:**
- Primary Key: `invoice_id`
- Indexes: by date, by vendor+date
- Contains: header info, totals, metadata

**retail-invoice-line-items table:**
- Primary Key: `invoice_id` + `line_number`
- Indexes: by brand, by product_type
- Contains: product details, pricing, quantities

## Support

For issues:
1. Check the error messages in the upload UI
2. Verify AWS credentials and permissions
3. Review [INVOICE_ANALYTICS_README.md](INVOICE_ANALYTICS_README.md) for detailed docs
4. Check CloudWatch Logs in AWS console

---

**Happy uploading!** 🚀
