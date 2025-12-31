# Streamlit Cloud Deployment - Invoice Analytics

## ✅ Pre-Deployment Checklist

All code has been pushed to GitHub and is ready for Streamlit Cloud deployment.

## 📋 Streamlit Cloud Configuration

### 1. Secrets Configuration

In Streamlit Cloud dashboard, add these secrets:

```toml
[passwords]
admin = "f0ec997a876e18b3bbe90cd22d67b91f93329dff3ea28b58c613e05cab4971d9"
analyst = "f0ec997a876e18b3bbe90cd22d67b91f93329dff3ea28b58c613e05cab4971d9"

[aws]
access_key_id = "YOUR_AWS_ACCESS_KEY"
secret_access_key = "YOUR_AWS_SECRET_KEY"
region = "us-west-1"
bucket_name = "retail-data-bcgr"

[anthropic]
ANTHROPIC_API_KEY = "YOUR_CLAUDE_API_KEY"

[dynamodb]
dynamodb_qr_table = "qr-tracker-qr-codes"
dynamodb_clicks_table = "qr-tracker-clicks"

[redirect-url]
redirect_base_url = "https://skhaq1xs3j.execute-api.us-west-1.amazonaws.com/prod/r"
```

### 2. AWS IAM Permissions Required

Your AWS user needs this additional DynamoDB policy for invoice analytics:

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
            "dynamodb:Query",
            "dynamodb:BatchWriteItem"
        ],
        "Resource": [
            "arn:aws:dynamodb:us-west-1:*:table/retail-invoices",
            "arn:aws:dynamodb:us-west-1:*:table/retail-invoice-line-items",
            "arn:aws:dynamodb:us-west-1:*:table/retail-invoice-aggregations",
            "arn:aws:dynamodb:us-west-1:*:table/retail-invoices/index/*",
            "arn:aws:dynamodb:us-west-1:*:table/retail-invoice-line-items/index/*"
        ]
    }]
}
```

### 3. Python Version

Ensure Streamlit Cloud is using Python 3.9 or later (specified in runtime.txt if needed).

## 🚀 After Deployment

### Initialize DynamoDB Tables

Once deployed, create the invoice tables by running this Python code in the Streamlit app or via a one-time script:

```python
from invoice_extraction import InvoiceDataService
import streamlit as st

service = InvoiceDataService(
    aws_access_key=st.secrets['aws']['access_key_id'],
    aws_secret_key=st.secrets['aws']['secret_access_key'],
    region=st.secrets['aws']['region']
)

service.create_tables()
```

This will create:
- `retail-invoices` table
- `retail-invoice-line-items` table
- `retail-invoice-aggregations` table

### Load Invoice Data

You can load invoices either:

**Option A: Upload via Streamlit Interface**
- Add file uploader to app.py
- Process PDFs on upload

**Option B: Load from Local Machine**
```bash
# From your local machine with invoices
python3 -c "
from invoice_extraction import InvoiceDataService
import streamlit as st

service = InvoiceDataService(
    aws_access_key='YOUR_KEY',
    aws_secret_key='YOUR_SECRET',
    region='us-west-1'
)

# Load single invoice
from invoice_extraction import TreezInvoiceParser
parser = TreezInvoiceParser()
data = parser.extract_from_pdf('invoice.pdf')
service.store_invoice(data)
"
```

## 📊 Using Invoice Analytics in App

Add to your app.py:

```python
from invoice_analytics import get_invoice_analytics_client

# In your main dashboard code
st.header("📋 Invoice Analytics")

analytics = get_invoice_analytics_client()

if analytics:
    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("Vendor Analysis"):
            with st.spinner("Analyzing vendor spending..."):
                result = analytics.analyze_vendor_spending()
                st.markdown(result)

    with col2:
        if st.button("Product Analysis"):
            with st.spinner("Analyzing product purchasing..."):
                result = analytics.analyze_product_purchasing()
                st.markdown(result)

    with col3:
        if st.button("Pricing Analysis"):
            with st.spinner("Analyzing pricing..."):
                result = analytics.analyze_pricing_margins()
                st.markdown(result)
else:
    st.warning("Invoice analytics not available. Check AWS credentials.")
```

## 💰 Cost Monitoring

After deployment, monitor costs at:
- **DynamoDB**: AWS Console > DynamoDB > Tables
  - Expected: $2-3/month for 3 tables with 5 RCU/5 WCU each
- **Claude API**: Anthropic Console > Usage
  - Expected: $1-5 per analysis query (vs $50-200 for extraction)

## 🔍 Troubleshooting

### "AccessDeniedException" from DynamoDB
- Verify IAM policy is attached to your AWS user
- Check that region matches (us-west-1)
- Ensure table names match in policy

### "Module not found" errors
- Check requirements.txt includes PyPDF2>=3.0.0
- Verify Streamlit Cloud rebuilt after adding dependencies

### No data in tables
- Confirm tables were created successfully
- Check CloudWatch Logs for DynamoDB errors
- Verify invoices were actually loaded

## 📖 Documentation

See [INVOICE_ANALYTICS_README.md](INVOICE_ANALYTICS_README.md) for:
- Complete API reference
- Cost analysis details
- Integration examples
- Customization options

## ✅ Deployment Complete

Once secrets are configured and tables are created, your invoice analytics will be live!

The system will:
- ✅ Parse Treez invoices without Claude API costs
- ✅ Store data efficiently in DynamoDB
- ✅ Provide Claude-powered insights on demand
- ✅ Save 89% on API costs vs traditional extraction
