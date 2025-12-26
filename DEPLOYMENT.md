# Industry Research Agent - Deployment Guide

Automated Lambda function that monitors cannabis industry trends, regulations, and competitive landscape.

## Prerequisites

- Docker installed and running
- AWS CLI configured with credentials
- Anthropic API key

## Quick Deploy

```bash
export ANTHROPIC_API_KEY="sk-ant-your-key-here"
export AWS_REGION="us-west-1"  # Optional, defaults to us-west-1
export S3_BUCKET_NAME="retail-data-bcgr"  # Optional

./deploy-research-agent.sh
```

This script will:
1. Create IAM role with necessary permissions
2. Setup ECR repository
3. Build Docker image for Lambda (linux/amd64)
4. Push image to ECR
5. Create/update Lambda function
6. Setup EventBridge schedule (runs every 6 hours by default)

## Update Schedule

Change how often the agent runs:

```bash
# Every 12 hours
./update-schedule.sh 'rate(12 hours)'

# Once per day
./update-schedule.sh 'rate(1 day)'

# Daily at 9am UTC
./update-schedule.sh 'cron(0 9 * * ? *)'
```

## Manual Testing

```bash
# Test with specific topic
aws lambda invoke \
  --function-name industry-research-agent \
  --payload '{"topics":["regulatory"]}' \
  --cli-binary-format raw-in-base64-out \
  --region us-west-1 \
  output.json

# View results
cat output.json

# Force full research (skip preliminary scans)
aws lambda invoke \
  --function-name industry-research-agent \
  --payload '{"force_full":true}' \
  --cli-binary-format raw-in-base64-out \
  --region us-west-1 \
  output.json
```

## View Research Findings

Findings are stored in S3:

```bash
# List all findings
aws s3 ls s3://retail-data-bcgr/research-findings/ --recursive

# Download latest summary
aws s3 cp s3://retail-data-bcgr/research-findings/summary/latest.json ./latest-findings.json
```

## View Logs

```bash
# Tail logs in real-time
aws logs tail /aws/lambda/industry-research-agent --follow --region us-west-1

# View recent logs
aws logs tail /aws/lambda/industry-research-agent --since 1h --region us-west-1
```

## Architecture

- **Lambda Function**: `industry-research-agent`
  - Runtime: Container (Python 3.11)
  - Memory: 512MB
  - Timeout: 10 minutes
  - Region: us-west-1

- **EventBridge Schedule**: `industry-research-agent-schedule`
  - Default: Every 6 hours
  - Payload: `{"mode": "research"}`

- **IAM Role**: `industry-research-agent-role`
  - Permissions: Lambda execution, S3 full access

- **ECR Repository**: `industry-research-agent`
  - Contains Docker image with dependencies

## Research Topics

The agent monitors:
1. **Regulatory Updates** (high priority)
2. **Market Trends** (medium priority)
3. **Competitive Landscape** (medium priority)
4. **Product Innovation** (low priority)
5. **Pricing & Economics** (high priority)

## Updating the Function

After modifying `lambda_function.py` or `requirements.txt`:

```bash
./deploy-research-agent.sh
```

The script handles rebuilding and redeploying automatically.

## Troubleshooting

**Lambda function not running:**
```bash
# Check function status
aws lambda get-function --function-name industry-research-agent --region us-west-1

# Check schedule
aws events describe-rule --name industry-research-agent-schedule --region us-west-1
```

**Image build fails:**
```bash
# Ensure Docker is running
docker info

# Check platform specification
docker build --platform linux/amd64 -t test .
```

**Permission errors:**
```bash
# Verify role exists
aws iam get-role --role-name industry-research-agent-role

# Check role policies
aws iam list-attached-role-policies --role-name industry-research-agent-role
```

## Cost Optimization

- High-priority topics always run full research
- Medium/low priority topics use preliminary scans to skip redundant research
- Adjust schedule frequency based on needs
- Monitor CloudWatch costs and adjust timeout if needed

## Files

- `deploy-research-agent.sh` - Main deployment script
- `update-schedule.sh` - Update EventBridge schedule
- `lambda_function.py` - Research agent code
- `requirements.txt` - Python dependencies
- `Dockerfile` - Lambda container definition
- `cloudformation-container.yaml` - Alternative CloudFormation deployment (not used)
