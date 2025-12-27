#!/bin/bash
# ==============================================================================
# SEO Research Agent - Complete Deployment Script
# Builds Docker image, pushes to ECR, creates/updates Lambda function
# Configured for us-west-1 region with daily schedule
# ==============================================================================

set -e

# Configuration
REGION="us-west-1"
FUNCTION_NAME="seo-research-agent-analyzer"
ROLE_NAME="seo-research-agent-role"
ECR_REPO="seo-research-agent"
SCHEDULE_RATE="rate(1 day)"
S3_BUCKET="${S3_BUCKET_NAME:-retail-data-bcgr}"
TARGET_WEBSITES="${TARGET_WEBSITES:-https://barbarycoastsf.com,https://grassrootssf.com}"

# Check prerequisites
check_prereqs() {
    echo "→ Checking prerequisites..."

    if [ -z "$ANTHROPIC_API_KEY" ]; then
        echo "✗ ANTHROPIC_API_KEY not set"
        echo "  Export it: export ANTHROPIC_API_KEY='sk-ant-...'"
        exit 1
    fi

    command -v docker >/dev/null 2>&1 || { echo "✗ Docker not found"; exit 1; }
    command -v aws >/dev/null 2>&1 || { echo "✗ AWS CLI not found"; exit 1; }
    docker info >/dev/null 2>&1 || { echo "✗ Docker not running"; exit 1; }
    aws sts get-caller-identity >/dev/null 2>&1 || { echo "✗ AWS credentials not configured"; exit 1; }

    echo "✓ All prerequisites met"
}

# Get AWS account info
get_account_info() {
    ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
    ECR_URI="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"
    IMAGE_URI="${ECR_URI}/${ECR_REPO}:latest"
    echo "  Account: $ACCOUNT_ID"
    echo "  Region: $REGION"
}

# Create IAM role if needed
setup_iam_role() {
    echo "→ Setting up IAM role..."

    if aws iam get-role --role-name $ROLE_NAME >/dev/null 2>&1; then
        echo "✓ Role already exists"
        return
    fi

    TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'

    aws iam create-role --role-name $ROLE_NAME --assume-role-policy-document "$TRUST_POLICY" >/dev/null
    aws iam attach-role-policy --role-name $ROLE_NAME --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
    aws iam attach-role-policy --role-name $ROLE_NAME --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess

    echo "✓ Role created"
    echo "  Waiting 15 seconds for propagation..."
    sleep 15
}

# Setup ECR repository
setup_ecr() {
    echo "→ Setting up ECR repository..."

    if aws ecr describe-repositories --repository-names $ECR_REPO --region $REGION >/dev/null 2>&1; then
        echo "✓ Repository exists"
    else
        aws ecr create-repository --repository-name $ECR_REPO --region $REGION >/dev/null
        echo "✓ Repository created"
    fi
}

# Build and push Docker image
build_and_push() {
    echo "→ Building Docker image for linux/amd64..."

    # Create a temporary Dockerfile for SEO agent
    cat > Dockerfile.seo << 'DOCKERFILE'
FROM public.ecr.aws/lambda/python:3.11

# Copy requirements and install
COPY requirements.txt ${LAMBDA_TASK_ROOT}/
RUN pip install --no-cache-dir -r requirements.txt

# Copy SEO lambda function
COPY seo_lambda_function.py ${LAMBDA_TASK_ROOT}/lambda_function.py

# Set handler
CMD ["lambda_function.lambda_handler"]
DOCKERFILE

    docker build --platform linux/amd64 -f Dockerfile.seo -t ${ECR_REPO}:latest .
    rm Dockerfile.seo
    echo "✓ Image built"

    echo "→ Pushing to ECR..."
    aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $ECR_URI
    docker tag ${ECR_REPO}:latest $IMAGE_URI
    docker push $IMAGE_URI
    echo "✓ Image pushed"
}

# Get the correct image digest
get_image_digest() {
    echo "→ Getting image digest..."
    DIGEST=$(aws ecr describe-images --repository-name $ECR_REPO --region $REGION \
        --query 'imageDetails[?imageManifestMediaType==`application/vnd.oci.image.manifest.v1+json`]|[0].imageDigest' \
        --output text)
    IMAGE_URI_WITH_DIGEST="${ECR_URI}/${ECR_REPO}@${DIGEST}"
    echo "  Using: $DIGEST"
}

# Create or update Lambda function
deploy_lambda() {
    echo "→ Deploying Lambda function..."

    ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"

    if aws lambda get-function --function-name $FUNCTION_NAME --region $REGION >/dev/null 2>&1; then
        echo "  Updating existing function..."
        aws lambda update-function-code \
            --function-name $FUNCTION_NAME \
            --image-uri $IMAGE_URI_WITH_DIGEST \
            --region $REGION >/dev/null

        echo "  Waiting for function update to complete..."
        aws lambda wait function-updated \
            --function-name $FUNCTION_NAME \
            --region $REGION

        # Update environment variables
        aws lambda update-function-configuration \
            --function-name $FUNCTION_NAME \
            --environment Variables="{S3_BUCKET_NAME=$S3_BUCKET,S3_PREFIX=seo-analysis,TARGET_WEBSITES=\"$TARGET_WEBSITES\",ANTHROPIC_API_KEY=$ANTHROPIC_API_KEY,TOKENS_PER_MINUTE_LIMIT=30000,SAFETY_MARGIN=0.85}" \
            --region $REGION >/dev/null
    else
        echo "  Creating new function..."
        aws lambda create-function \
            --function-name $FUNCTION_NAME \
            --package-type Image \
            --code ImageUri=$IMAGE_URI_WITH_DIGEST \
            --role $ROLE_ARN \
            --timeout 900 \
            --memory-size 1024 \
            --environment Variables="{S3_BUCKET_NAME=$S3_BUCKET,S3_PREFIX=seo-analysis,TARGET_WEBSITES=\"$TARGET_WEBSITES\",ANTHROPIC_API_KEY=$ANTHROPIC_API_KEY,TOKENS_PER_MINUTE_LIMIT=30000,SAFETY_MARGIN=0.85}" \
            --region $REGION >/dev/null
    fi

    echo "✓ Lambda deployed"
}

# Setup EventBridge schedule
setup_schedule() {
    echo "→ Setting up schedule ($SCHEDULE_RATE)..."

    RULE_NAME="${FUNCTION_NAME}-daily"
    RULE_ARN="arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${RULE_NAME}"
    LAMBDA_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}"

    # Create/update the daily analysis rule
    aws events put-rule \
        --name $RULE_NAME \
        --schedule-expression "$SCHEDULE_RATE" \
        --state ENABLED \
        --region $REGION >/dev/null

    aws lambda add-permission \
        --function-name $FUNCTION_NAME \
        --statement-id AllowEventBridgeInvoke \
        --action lambda:InvokeFunction \
        --principal events.amazonaws.com \
        --source-arn $RULE_ARN \
        --region $REGION >/dev/null 2>&1 || true

    aws events put-targets \
        --rule $RULE_NAME \
        --targets "[{\"Id\":\"1\",\"Arn\":\"$LAMBDA_ARN\",\"Input\":\"{\\\"mode\\\":\\\"analyze\\\"}\"}]" \
        --region $REGION >/dev/null

    echo "✓ Daily analysis schedule configured"

    # Setup monthly archival
    ARCHIVAL_RULE_NAME="${FUNCTION_NAME}-monthly-archival"
    ARCHIVAL_RULE_ARN="arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${ARCHIVAL_RULE_NAME}"

    aws events put-rule \
        --name $ARCHIVAL_RULE_NAME \
        --schedule-expression "cron(0 6 1 * ? *)" \
        --state ENABLED \
        --region $REGION >/dev/null

    aws lambda add-permission \
        --function-name $FUNCTION_NAME \
        --statement-id AllowEventBridgeInvokeArchival \
        --action lambda:InvokeFunction \
        --principal events.amazonaws.com \
        --source-arn $ARCHIVAL_RULE_ARN \
        --region $REGION >/dev/null 2>&1 || true

    aws events put-targets \
        --rule $ARCHIVAL_RULE_NAME \
        --targets "[{\"Id\":\"1\",\"Arn\":\"$LAMBDA_ARN\",\"Input\":\"{\\\"mode\\\":\\\"archive\\\",\\\"delete_after_archive\\\":false}\"}]" \
        --region $REGION >/dev/null

    echo "✓ Monthly archival schedule configured"
}

# Main deployment
main() {
    echo ""
    echo "========================================"
    echo "SEO Research Agent Deployment"
    echo "Region: us-west-1"
    echo "Schedule: Daily (rate(1 day))"
    echo "Websites: $TARGET_WEBSITES"
    echo "========================================"
    echo ""

    check_prereqs
    get_account_info
    setup_iam_role
    setup_ecr
    build_and_push
    get_image_digest
    deploy_lambda
    setup_schedule

    echo ""
    echo "========================================"
    echo "✓ Deployment Complete!"
    echo "========================================"
    echo ""
    echo "Function: $FUNCTION_NAME"
    echo "Schedule: $SCHEDULE_RATE (daily at UTC midnight)"
    echo "Region: $REGION"
    echo "Websites: $TARGET_WEBSITES"
    echo ""
    echo "Commands:"
    echo "  # Test manually"
    echo "  aws lambda invoke --function-name $FUNCTION_NAME --payload '{\"mode\":\"analyze\"}' --cli-binary-format raw-in-base64-out --region $REGION out.json && cat out.json | jq"
    echo ""
    echo "  # View logs"
    echo "  aws logs tail /aws/lambda/$FUNCTION_NAME --follow --region $REGION"
    echo ""
    echo "  # Check S3 findings"
    echo "  aws s3 ls s3://$S3_BUCKET/seo-analysis/ --recursive"
    echo ""
}

main "$@"
