#!/bin/bash
# ==============================================================================
# Industry Research Agent - Complete Deployment Script
# Builds Docker image, pushes to ECR, creates/updates Lambda function
# ==============================================================================

set -e

# Configuration
REGION="${AWS_REGION:-us-west-1}"
FUNCTION_NAME="industry-research-agent"
ROLE_NAME="industry-research-agent-role"
ECR_REPO="industry-research-agent"
SCHEDULE_RATE="${SCHEDULE_RATE:-rate(6 hours)}"
S3_BUCKET="${S3_BUCKET_NAME:-retail-data-bcgr}"

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
    
    cat > /tmp/trust-policy.json << POLICY
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "lambda.amazonaws.com"},
    "Action": "sts:AssumeRole"
  }]
}
POLICY
    
    aws iam create-role --role-name $ROLE_NAME --assume-role-policy-document file:///tmp/trust-policy.json >/dev/null
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
    docker build --platform linux/amd64 -t ${ECR_REPO}:latest .
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
    else
        echo "  Creating new function..."
        aws lambda create-function \
            --function-name $FUNCTION_NAME \
            --package-type Image \
            --code ImageUri=$IMAGE_URI_WITH_DIGEST \
            --role $ROLE_ARN \
            --timeout 600 \
            --memory-size 512 \
            --environment "Variables={S3_BUCKET_NAME=$S3_BUCKET,ANTHROPIC_API_KEY=$ANTHROPIC_API_KEY}" \
            --region $REGION >/dev/null
    fi
    
    echo "✓ Lambda deployed"
}

# Setup EventBridge schedule
setup_schedule() {
    echo "→ Setting up schedule ($SCHEDULE_RATE)..."
    
    RULE_NAME="${FUNCTION_NAME}-schedule"
    RULE_ARN="arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${RULE_NAME}"
    LAMBDA_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}"
    
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
        --targets "[{\"Id\":\"1\",\"Arn\":\"$LAMBDA_ARN\",\"Input\":\"{\\\"mode\\\":\\\"research\\\"}\"}]" \
        --region $REGION >/dev/null
    
    echo "✓ Schedule configured"
}

# Main deployment
main() {
    echo ""
    echo "========================================"
    echo "Industry Research Agent Deployment"
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
    echo "Schedule: $SCHEDULE_RATE"
    echo "Region: $REGION"
    echo ""
    echo "Commands:"
    echo "  # Test manually"
    echo "  aws lambda invoke --function-name $FUNCTION_NAME --payload '{\"topics\":[\"regulatory\"]}' --cli-binary-format raw-in-base64-out --region $REGION out.json"
    echo ""
    echo "  # View logs"
    echo "  aws logs tail /aws/lambda/$FUNCTION_NAME --follow --region $REGION"
    echo ""
    echo "  # Check S3 findings"
    echo "  aws s3 ls s3://$S3_BUCKET/research-findings/ --recursive"
    echo ""
}

main "$@"
