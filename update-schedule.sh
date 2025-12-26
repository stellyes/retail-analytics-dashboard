#!/bin/bash
# ==============================================================================
# Update Research Agent Schedule
# Easily change how often the research agent runs
# ==============================================================================

REGION="${AWS_REGION:-us-west-1}"
RULE_NAME="industry-research-agent-schedule"

if [ -z "$1" ]; then
    echo "Usage: ./update-schedule.sh <schedule-expression>"
    echo ""
    echo "Examples:"
    echo "  ./update-schedule.sh 'rate(6 hours)'   # Every 6 hours"
    echo "  ./update-schedule.sh 'rate(12 hours)'  # Every 12 hours"
    echo "  ./update-schedule.sh 'rate(1 day)'     # Once per day"
    echo "  ./update-schedule.sh 'rate(7 days)'    # Once per week"
    echo "  ./update-schedule.sh 'cron(0 9 * * ? *)'  # Daily at 9am UTC"
    echo ""
    exit 1
fi

SCHEDULE="$1"

echo "Updating schedule to: $SCHEDULE"

aws events put-rule \
    --name $RULE_NAME \
    --schedule-expression "$SCHEDULE" \
    --state ENABLED \
    --region $REGION

echo "✓ Schedule updated successfully"
echo ""
echo "Verify with:"
echo "  aws events describe-rule --name $RULE_NAME --region $REGION"
