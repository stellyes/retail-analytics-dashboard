# 📊 Retail Analytics Dashboard

A comprehensive retail analytics platform for cannabis dispensaries with AI-powered industry research capabilities.

## Overview

This repository contains two main components:

1. **Streamlit Dashboard** (`app.py`) - Interactive analytics dashboard for retail sales data
2. **Research Agent** (`lambda_function.py`) - Autonomous AWS Lambda agent for industry research

### Dashboard Features

- 📈 **Multi-store analytics** for Barbary Coast and Grass Roots locations
- 🔒 **Password authentication** with role-based access
- ☁️ **S3 integration** for data persistence and CSV uploads
- 🤖 **Claude AI integration** for natural language analytics queries
- 🔬 **Industry research** page displaying automated research findings
- 📊 **Sales analysis** with trends, top products, brand performance
- 🏷️ **Product mapping** tools for data normalization
- 💰 **Promotional analysis** and discount tracking

### Research Agent Features

- ⏰ **Automated scheduling** (runs every 6 hours via EventBridge)
- 🧠 **Intelligent throttling** with automatic rate limit management
- 🔍 **Smart query selection** (2 random queries per cycle from 10 total)
- 📝 **Topic monitoring**: Regulations, market trends, competition, products, pricing
- 💾 **S3 storage** with cumulative summaries and historical archives
- 🔄 **Retry logic** for handling API rate limits gracefully

## Architecture

```
┌─────────────────────┐          ┌──────────────────────┐
│   Streamlit App     │          │   EventBridge        │
│   (app.py)          │          │   (Schedule)         │
└──────────┬──────────┘          └──────────┬───────────┘
           │                                 │
           │                                 ▼
           │                      ┌──────────────────────┐
           │                      │  Lambda Container    │
           │                      │  (Research Agent)    │
           │                      └──────────┬───────────┘
           │                                 │
           └────────────────┬────────────────┘
                            ▼
                 ┌──────────────────────┐
                 │         S3           │
                 │  • Sales CSVs        │
                 │  • Research findings │
                 │  • Product mappings  │
                 └──────────┬───────────┘
                            │
                            ▼
                 ┌──────────────────────┐
                 │    Claude API        │
                 │  • Analytics         │
                 │  • Web Search        │
                 └──────────────────────┘
```

## Project Structure

```
retail-analytics-dashboard/
├── app.py                      # Main Streamlit dashboard
├── claude_integration.py       # Claude AI analytics integration
├── research_integration.py     # Research findings display page
├── lambda_function.py          # AWS Lambda research agent
├── Dockerfile                  # Lambda container definition
├── requirements.txt            # Python dependencies
├── deploy-research-agent.sh    # Research agent deployment script
├── update-schedule.sh          # Schedule modification utility
├── DEPLOYMENT.md              # Research agent deployment guide
├── cloudformation.yaml         # AWS infrastructure (legacy)
├── cloudformation-container.yaml # Container-based infrastructure
├── dashboard_patch_instructions.py # Dashboard integration guide
└── README.md
```

## Quick Start

### 1. Streamlit Dashboard

**Prerequisites:**
- Python 3.11+
- AWS credentials configured
- Anthropic API key (for Claude features)

**Setup:**

```bash
# Install dependencies
pip install -r requirements.txt

# Configure secrets
# Create .streamlit/secrets.toml with:
# [passwords]
# admin = "your-hashed-password"
# analyst = "your-hashed-password"

# Run dashboard
streamlit run app.py
```

**Access:**
- Navigate to `http://localhost:8501`
- Login with configured credentials
- Upload CSV files or connect to S3 bucket

### 2. Research Agent (AWS Lambda)

**Prerequisites:**
- AWS CLI configured
- Docker installed
- Anthropic API key set in Lambda environment

**Deploy:**

```bash
# One-command deployment
./deploy-research-agent.sh
```

This deploys a containerized Lambda function that:
1. Runs every 6 hours automatically
2. Researches 2 random queries from 5 topics (10 total queries)
3. Stores findings in S3: `s3://retail-data-bcgr/research-findings/`
4. Handles API rate limits with intelligent throttling

See [DEPLOYMENT.md](DEPLOYMENT.md) for detailed instructions.

## Configuration

### Dashboard Settings

Edit `.streamlit/secrets.toml`:

```toml
[passwords]
admin = "hashed-password-here"
analyst = "hashed-password-here"

[aws]
bucket_name = "retail-data-bcgr"
region = "us-west-1"
```

### Research Agent Settings

**Schedule:**
```bash
# Change research frequency
./update-schedule.sh 12  # Every 12 hours
./update-schedule.sh 6   # Every 6 hours (default)
```

**Topics & Queries:**

Edit `RESEARCH_TOPICS` in `lambda_function.py`:

```python
RESEARCH_TOPICS = [
    {
        "id": "regulatory",
        "name": "Regulatory Updates",
        "queries": [
            "cannabis regulation California 2025",
            "marijuana dispensary license California"
        ],
        "importance": "high"
    },
    # 4 more topics...
]
```

**Throttling:**

Default settings in `lambda_function.py`:
- `max_queries = 2` - Research 2 queries per cycle
- `safety_margin = 0.85` - Use 85% of rate limit (25,500 tokens/min)
- Automatic retry with 65-second wait on rate limits

## Usage

### Dashboard Analytics

1. **Upload Data**: Use sidebar to upload CSV files or select S3 files
2. **View Metrics**: Monitor sales trends, top products, brand performance
3. **Product Mapping**: Normalize product names for better analytics
4. **AI Analysis**: Ask questions in natural language via Claude integration
5. **Research**: View industry research findings on dedicated page

### Research Agent

**Manual Invocation:**
```bash
# Trigger research manually
aws lambda invoke \
  --function-name industry-research-agent \
  --region us-west-1 \
  response.json

# View latest findings
aws s3 cp s3://retail-data-bcgr/research-findings/$(date +%Y/%m/%d)/findings.json - \
  --region us-west-1 | python -m json.tool
```

**Monitoring:**
```bash
# Check token usage and stats
aws s3 cp s3://retail-data-bcgr/research-findings/$(date +%Y/%m/%d)/findings.json - \
  --region us-west-1 | python -c "
import json, sys
d = json.load(sys.stdin)
print(f'Topics researched: {d[\"topics_researched\"]}')
print(f'Findings collected: {sum(len(t.get(\"findings\", [])) for t in d[\"topics\"])}')
print(f'Throttle stats: {d.get(\"throttle_stats\", {})}')"
```

## Data Storage (S3)

```
s3://retail-data-bcgr/
├── sales-data/                     # Dashboard CSV uploads
│   ├── barbary_coast/
│   └── grass_roots/
├── product-mappings/               # Product normalization data
│   └── mappings.json
└── research-findings/              # Research agent output
    ├── 2025/12/27/findings.json   # Daily findings
    ├── summary/latest.json         # Current summary
    └── archive/                    # Historical archives
        └── historical-context.json
```

## Cost Estimate

### Dashboard
- **Streamlit Cloud**: $0-20/month (depending on usage)
- **AWS S3**: ~$1-5/month for storage
- **Claude API** (analytics): ~$5-10/month (pay-per-use)

### Research Agent
- **Lambda**: ~$0.50/month (minimal compute)
- **EventBridge**: Free (under 1M events)
- **ECR**: ~$0.10/month for image storage
- **Claude API** (research): ~$10-20/month
  - Quiet day: ~$0.01/cycle
  - Normal day: ~$0.05-0.10/cycle
  - Busy news: ~$0.15/cycle
  - 4 cycles/day × 30 days = ~$6-18/month

**Total: ~$20-50/month**

## Updating

### Dashboard Changes

```bash
# Local development
streamlit run app.py

# Deploy to Streamlit Cloud
git push origin main
```

### Research Agent Updates

```bash
# Rebuild and redeploy
./deploy-research-agent.sh

# Or manually:
docker build --platform linux/amd64 --provenance=false --sbom=false \
  -t industry-research-agent .

docker tag industry-research-agent:latest \
  716121312511.dkr.ecr.us-west-1.amazonaws.com/industry-research-agent:latest

docker push 716121312511.dkr.ecr.us-west-1.amazonaws.com/industry-research-agent:latest

aws lambda update-function-code \
  --function-name industry-research-agent \
  --image-uri 716121312511.dkr.ecr.us-west-1.amazonaws.com/industry-research-agent:latest \
  --region us-west-1
```

## Security

**Important Notes:**
- Never commit secrets to git
- Use `.gitignore` (configured) to exclude:
  - `.claude/` - AI assistant files
  - `*.json` - Test/response files
  - `*.env` - Environment files
  - `.streamlit/secrets.toml` - Dashboard credentials
- Store API keys in AWS Lambda environment variables
- Use IAM roles with least-privilege permissions
- Hash passwords in `secrets.toml`

## Troubleshooting

### Dashboard Issues

**Problem**: "Could not connect to S3"
- Check AWS credentials: `aws s3 ls`
- Verify bucket exists: `aws s3 ls s3://retail-data-bcgr/`
- Check region in `.streamlit/secrets.toml`

**Problem**: "Claude integration not working"
- Verify `claude_integration.py` exists
- Check Anthropic API key in environment

### Research Agent Issues

**Problem**: Rate limit errors (429)
- Default: 2 queries per cycle with 65s retry wait
- Reduce queries: Change `max_queries = 1` in `lambda_function.py`
- See troubleshooting in [DEPLOYMENT.md](DEPLOYMENT.md)

**Problem**: Lambda timeout
- Check CloudWatch logs
- Current timeout: 600s (10 minutes)
- Each query takes ~30-60 seconds

**Problem**: No findings generated
- Check S3 path: `research-findings/YYYY/MM/DD/findings.json`
- View Lambda logs for errors
- Verify Anthropic API key in Lambda environment

## Additional Documentation

- [DEPLOYMENT.md](DEPLOYMENT.md) - Research agent deployment guide
- Research agent uses Claude with web search tool
- Dashboard integrates with S3, Claude API, and research findings
- All research data persists in S3 with historical archival

## License

Internal use only - Cannabis retail analytics platform
