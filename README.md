# 🔬 Cannabis Industry Research Agent

An autonomous AI-powered research agent that monitors cannabis industry trends, regulations, and market developments. Runs on AWS Lambda (container image) and integrates with your existing Streamlit retail dashboard.

## 📋 Overview

The research agent:
- **Runs automatically** on a configurable schedule (default: every 8 hours)
- **Smart scanning**: Uses cheap Haiku scan first, only does full Sonnet research if new content found
- **Monitors key topics**: Regulations, market trends, competition, products, pricing
- **Maintains context**: Reviews prior findings before each research cycle
- **Stores findings** in S3 with cumulative summaries
- **Integrates** with your existing Streamlit dashboard

## 🏗️ Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   EventBridge   │────▶│  Lambda (ECR)   │────▶│       S3        │
│   (Schedule)    │     │  Container      │     │ (Findings Store)│
└─────────────────┘     └────────┬────────┘     └────────┬────────┘
                                 │                        │
                                 ▼                        ▼
                        ┌─────────────────┐     ┌─────────────────┐
                        │  Claude API     │     │   Streamlit     │
                        │  (Web Search)   │     │   Dashboard     │
                        └─────────────────┘     └─────────────────┘
```

## 📁 Project Structure

```
industry_research_agent/
├── lambda_function.py          # Main research agent
├── Dockerfile                  # Lambda container definition
├── requirements.txt            # Python dependencies
├── cloudformation-container.yaml  # AWS infrastructure
├── deploy-container.sh         # One-command deployment
├── research_integration.py     # Streamlit dashboard integration
└── README.md
```

## 🚀 Quick Start

### Prerequisites

1. **AWS CLI** installed and configured
2. **Docker** installed and running
3. **Anthropic API Key** from [console.anthropic.com](https://console.anthropic.com)

### Deploy

```bash
# Set your API key
export ANTHROPIC_API_KEY="sk-ant-api03-..."

# Optional: customize settings
export S3_BUCKET_NAME="retail-data-bcgr"
export AWS_REGION="us-west-2"
export SCHEDULE="rate(8 hours)"

# Deploy
chmod +x deploy-container.sh
./deploy-container.sh
```

This will:
1. Create an ECR repository
2. Build the Docker image
3. Push to ECR
4. Deploy CloudFormation stack
5. Set up EventBridge schedule

### Integrate with Dashboard

Add to your `dashboard.py`:

```python
from research_integration import render_research_page

# In navigation, add: "🔬 Industry Research"
# In page routing:
elif page == "🔬 Industry Research":
    render_research_page()
```

## ⚙️ Configuration

### Schedule Options

| Expression | Description |
|------------|-------------|
| `rate(8 hours)` | Every 8 hours (default) |
| `rate(4 hours)` | Every 4 hours |
| `rate(1 day)` | Once daily |
| `cron(0 8,16 * * ? *)` | 8am and 4pm UTC |

### Research Topics

Edit `RESEARCH_TOPICS` in `lambda_function.py`:

```python
RESEARCH_TOPICS = [
    {
        "id": "regulatory",
        "name": "Regulatory Updates",
        "queries": ["cannabis regulation California", ...],
        "importance": "high"  # high = always full research
    },
    ...
]
```

## 💰 Cost Optimization

The agent uses smart scanning to reduce costs:

1. **Quick scan** (Haiku, ~$0.002) checks if there's new content
2. **Full research** (Sonnet, ~$0.05) only runs if new content found
3. **High-importance topics** (regulatory, pricing) always get full research

| Scenario | Cost per cycle |
|----------|---------------|
| Quiet day | ~$0.06 |
| Normal day | ~$0.12 |
| Busy news | ~$0.26 |

**Monthly estimate**: ~$10-15 at 3x daily

## 🔧 Manual Commands

```bash
# Trigger research
aws lambda invoke --function-name industry-research-agent-research-agent \
  --payload '{"mode": "research"}' out.json

# Force full research (skip scans)
aws lambda invoke --function-name industry-research-agent-research-agent \
  --payload '{"mode": "research", "force_full": true}' out.json

# Research specific topics
aws lambda invoke --function-name industry-research-agent-research-agent \
  --payload '{"mode": "research", "topics": ["regulatory", "pricing"]}' out.json

# Run archival (condense old data)
aws lambda invoke --function-name industry-research-agent-research-agent \
  --payload '{"mode": "archive"}' out.json

# Archive and delete original daily files
aws lambda invoke --function-name industry-research-agent-research-agent \
  --payload '{"mode": "archive", "delete_after_archive": true}' out.json

# View logs
aws logs tail /aws/lambda/industry-research-agent-research-agent --follow
```

## 📦 Data Archival & Historical Context

The agent automatically maintains long-term memory through a condensation system:

### How It Works

1. **Daily findings** are stored in `research-findings/YYYY/MM/DD/findings.json`
2. **After 30 days**, findings are condensed into monthly archives
3. **Monthly archives** are synthesized into a **historical context document**
4. **The agent reads historical context** before each research cycle

### Automatic Schedules

| Schedule | Action |
|----------|--------|
| Every 8 hours (default) | Research cycle |
| 1st of each month, 6am UTC | Archival cycle |

### S3 Data Structure

```
s3://your-bucket/research-findings/
├── 2024/01/15/findings.json      # Daily (deleted after 30 days if configured)
├── summary/
│   ├── latest.json               # Current state
│   └── history.json              # Recent history
└── archive/
    ├── 2024/01/monthly-summary.json   # January 2024 condensed
    ├── 2024/02/monthly-summary.json   # February 2024 condensed
    └── historical-context.json        # Long-term trends document
```

### Historical Context Document

The agent maintains a living document with:
- **Industry overview**: Current state and trajectory
- **Long-term trends**: Regulatory, market, pricing directions
- **Historical timeline**: Major events and turning points
- **Ongoing stories**: Narratives to track over time
- **Lessons learned**: Patterns from historical data

This gives the agent deep context about where the industry has been and where it's heading.

## 🔄 Updating

After code changes:

```bash
./deploy-container.sh
```

This rebuilds the image and updates Lambda automatically.
