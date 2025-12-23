# 🌿 Retail Analytics Dashboard

A comprehensive Streamlit application for cannabis retail analytics, designed for multi-store operations with persistent S3 data storage.

## Features

- **📊 Dashboard Overview**: KPIs, sales trends, and store comparison
- **📈 Sales Analysis**: Daily trends, day-of-week patterns, margin tracking
- **🏷️ Brand Performance**: Top brands, margin analysis, underperformer identification
- **📦 Product Categories**: Category mix visualization and breakdown
- **💡 AI Recommendations**: Automated business insights and action items
- **🔐 Password Authentication**: Secure multi-user access
- **☁️ S3 Integration**: Persistent data storage with AWS S3

---

## Quick Start

### 1. Clone and Install

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Secrets

```bash
# Copy the example secrets file
cp .streamlit/secrets.toml.example .streamlit/secrets.toml

# Edit with your credentials
nano .streamlit/secrets.toml
```

### 3. Run Locally

```bash
streamlit run app.py
```

Default login:
- **Username**: `admin`
- **Password**: `changeme123`

---

## AWS S3 Setup

### Create S3 Bucket

1. Go to AWS S3 Console
2. Create bucket with name like `your-company-retail-analytics`
3. Recommended settings:
   - Region: `us-west-2` (or closest to you)
   - Block all public access: ✅ Enabled
   - Versioning: Optional but recommended

### Create IAM User

1. Go to AWS IAM Console
2. Create new user with programmatic access
3. Attach policy (create custom or use inline):

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:ListBucket",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::your-retail-analytics-bucket",
                "arn:aws:s3:::your-retail-analytics-bucket/*"
            ]
        }
    ]
}
```

4. Save the Access Key ID and Secret Access Key

### Configure Bucket Structure

The app organizes data as:

```
your-bucket/
├── raw-uploads/           # Original uploaded CSVs
│   ├── sales_20241201_143022.csv
│   ├── brand_20241201_143025.csv
│   └── product_20241201_143028.csv
├── processed/             # Cleaned/merged data
│   ├── combined/
│   ├── grass_roots/
│   └── barbary_coast/
```

---

## Deployment Options

### Option 1: Streamlit Community Cloud (Free)

1. Push code to GitHub (exclude secrets.toml!)
2. Go to [share.streamlit.io](https://share.streamlit.io)
3. Connect your repository
4. Add secrets in the Streamlit Cloud dashboard

### Option 2: AWS EC2

```bash
# Launch EC2 instance (t3.small recommended)
# SSH into instance

# Install Python 3.11+
sudo apt update
sudo apt install python3.11 python3.11-venv

# Clone your repo
git clone https://github.com/your-org/retail-analytics.git
cd retail-analytics

# Setup environment
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Create secrets file
mkdir -p .streamlit
nano .streamlit/secrets.toml

# Run with nohup (or use systemd/supervisor)
nohup streamlit run app.py --server.port 8501 --server.address 0.0.0.0 &
```

Configure security group to allow inbound traffic on port 8501.

### Option 3: Docker

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
```

```bash
docker build -t retail-analytics .
docker run -p 8501:8501 -e AWS_ACCESS_KEY_ID=xxx -e AWS_SECRET_ACCESS_KEY=xxx retail-analytics
```

---

## Configuration Reference

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `AWS_ACCESS_KEY_ID` | AWS access key | From secrets.toml |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key | From secrets.toml |
| `AWS_DEFAULT_REGION` | AWS region | `us-west-2` |
| `S3_BUCKET_NAME` | S3 bucket name | From secrets.toml |

### Adding New Users

1. Generate password hash:
```python
import hashlib
password = "new_user_password"
print(hashlib.sha256(password.encode()).hexdigest())
```

2. Add to `secrets.toml`:
```toml
[passwords]
new_user = "generated_hash_here"
```

---

## Data Format Requirements

### Sales by Store CSV
Required columns:
- `Store` - Store name
- `Date` - Transaction date
- `Tickets Count` - Number of transactions
- `Net Sales` - Revenue after discounts/returns
- `Gross Margin %` - Margin percentage
- Plus: Units Sold, Customers Count, New Customers, Discounts, Returns, COGS, etc.

### Net Sales by Brand CSV
Required columns:
- `Brand` - Brand name (may include store prefix like `[SS]`, `[DS]`)
- `Net Sales` - Total brand revenue
- `Gross Margin %` - Brand margin
- `% of Total Net Sales` - Share of total sales

### Net Sales by Product CSV
Required columns:
- `Product Type` - Category (FLOWER, PREROLL, CARTRIDGE, etc.)
- `Net Sales` - Category revenue

---

## Customization

### Store Mapping

Edit the `STORE_MAPPING` dict in `app.py`:

```python
STORE_MAPPING = {
    "Your Store Name - Location": "store_id",
    "[PREFIX]": "store_id",
}
```

### Adding New Visualizations

1. Create function in the Visualization Components section
2. Call from appropriate render function
3. Use Plotly for interactive charts

### Adding New Recommendations

Extend `AnalyticsEngine.generate_recommendations()`:

```python
if some_condition:
    recommendations.append({
        'type': 'opportunity',  # or 'warning', 'insight'
        'title': 'Your Recommendation Title',
        'description': 'Detailed explanation...',
        'priority': 'high'  # or 'medium', 'low'
    })
```

---

## Troubleshooting

### "S3 connection not configured"
- Check AWS credentials in secrets.toml
- Verify IAM user has correct permissions
- Test with AWS CLI: `aws s3 ls s3://your-bucket`

### Login not working
- Ensure password hash matches exactly
- Check secrets.toml is in `.streamlit/` directory
- Verify file permissions

### Charts not loading
- Check data was processed successfully
- Look for errors in browser console
- Verify CSV column names match expected format

---

## License

MIT License - Feel free to modify for your business needs.
