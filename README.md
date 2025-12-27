# 🔬 Cannabis Industry Research Agent

An autonomous AI-powered research agent that monitors cannabis industry trends, regulations, and market developments. Runs on AWS Lambda (container image) and stores findings in S3.

## 📋 Overview

The research agent:
- **Runs automatically** on a schedule (default: every 6 hours)
- **Intelligent throttling**: Automatically manages API rate limits with retry logic
- **Smart query selection**: Researches 2 random queries per cycle from all topics
- **Monitors key topics**: Regulations, market trends, competition, products, pricing
- **Maintains context**: Reviews prior findings before each research cycle
- **Stores findings** in S3 with cumulative summaries and historical archives
- **Rate limit safe**: Built-in token tracking and automatic pausing/retry

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
retail-analytics-dashboard/
├── lambda_function.py          # Main research agent with throttling
├── Dockerfile                  # Lambda container definition
├── requirements.txt            # Python dependencies
├── deploy-research-agent.sh    # Deployment script
├── update-schedule.sh          # Schedule modification utility
├── DEPLOYMENT.md              # Deployment guide
├── THROTTLING.md              # Token throttling documentation
├── RATE_LIMITS.md             # Rate limit management guide
└── README.md
```

## 🚀 Quick Start

### Prerequisites

1. **AWS CLI** installed and configured
2. **Docker** installed and running
3. **Anthropic API Key** from [console.anthropic.com](https://console.anthropic.com)

### Deploy

```bash
# Run the deployment script
./deploy-research-agent.sh
```

This will:
1. Create ECR repository (if needed)
2. Build Docker image with platform specification
3. Push to ECR
4. Create IAM role (if needed)
5. Deploy Lambda function
6. Set up EventBridge schedule (every 6 hours)

The script handles everything automatically. See [DEPLOYMENT.md](DEPLOYMENT.md) for detailed instructions.

### Test the Function

```bash
# Invoke manually
aws lambda invoke --function-name industry-research-agent --region us-west-1 response.json

# Check findings in S3
aws s3 ls s3://retail-data-bcgr/research-findings/ --recursive
```

## ⚙️ Configuration

### Schedule Options

Update the schedule using the utility script:

```bash
# Change to every 12 hours
./update-schedule.sh 12

# Change to every 4 hours
./update-schedule.sh 4
```

Current schedule: **Every 6 hours**

### Throttling Configuration

The agent includes intelligent token throttling to stay within API rate limits (30,000 tokens/minute). See [THROTTLING.md](THROTTLING.md) for details.

Default settings in `lambda_function.py`:
- `max_queries = 2` - Research 2 random queries per cycle
- `safety_margin = 0.85` - Use 85% of rate limit (25,500 tokens)
- Automatic retry with 65-second wait on rate limit errors

### Research Topics

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
    # ... 4 more topics (10 total queries)
]
```

The agent randomly selects 2 queries per cycle, ensuring all topics get coverage over time.

## 💰 Cost Optimization

The agent uses multiple strategies to reduce costs:

1. **Smart query selection**: Only 2 random queries per cycle (out of 10 total)
2. **Quick scan** (Haiku, ~$0.002) checks if there's new content
3. **Full research** (Sonnet, ~$0.05) only runs if new content found
4. **Intelligent throttling**: Prevents wasted API calls from rate limit errors
5. **Automatic retry**: Waits and retries instead of failing

| Scenario | Cost per cycle |
|----------|---------------|
| Quiet day (no new content) | ~$0.01 |
| Normal day (1-2 topics) | ~$0.05-0.10 |
| Busy news (all queries) | ~$0.15 |

**Monthly estimate**: ~$10-20 at 4x daily (every 6 hours)

## 🔧 Manual Commands

```bash
# Trigger research cycle
aws lambda invoke --function-name industry-research-agent \
  --region us-west-1 response.json

# View latest findings
aws s3 cp s3://retail-data-bcgr/research-findings/$(date +%Y/%m/%d)/findings.json - \
  --region us-west-1 | python -m json.tool

# View summary
aws s3 cp s3://retail-data-bcgr/research-findings/summary/latest.json - \
  --region us-west-1 | python -m json.tool

# Change schedule (using utility script)
./update-schedule.sh 12  # Change to every 12 hours
```

## 📊 Monitoring

Check token usage and research stats:

```bash
# View recent findings with throttle stats
aws s3 cp s3://retail-data-bcgr/research-findings/$(date +%Y/%m/%d)/findings.json - \
  --region us-west-1 | python -c "import json, sys; d=json.load(sys.stdin); \
  print(f'Topics researched: {d[\"topics_researched\"]}'); \
  print(f'Throttle stats: {d.get(\"throttle_stats\", {})}')"
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

After code changes, rebuild and redeploy:

```bash
# Rebuild and push Docker image
docker build --platform linux/amd64 --provenance=false --sbom=false \
  -t industry-research-agent .

docker tag industry-research-agent:latest \
  716121312511.dkr.ecr.us-west-1.amazonaws.com/industry-research-agent:latest

docker push 716121312511.dkr.ecr.us-west-1.amazonaws.com/industry-research-agent:latest

# Update Lambda function
aws lambda update-function-code \
  --function-name industry-research-agent \
  --image-uri 716121312511.dkr.ecr.us-west-1.amazonaws.com/industry-research-agent:latest \
  --region us-west-1
```

Or use the deployment script: `./deploy-research-agent.sh`

## 📚 Additional Documentation

- [DEPLOYMENT.md](DEPLOYMENT.md) - Detailed deployment instructions
- [THROTTLING.md](THROTTLING.md) - Token throttling system documentation
- [RATE_LIMITS.md](RATE_LIMITS.md) - Rate limit management strategies

## 🗂️ Repository Structure

Important: The `.gitignore` file is configured to exclude:
- `.claude/` - Claude Code assistant files
- `*.json` - Test and response files
- `*.env` - Environment/secret files

Never commit these files to version control.
