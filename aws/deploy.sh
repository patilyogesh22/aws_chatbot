#!/bin/bash

set -e

PROJECT="chatbot"
REGION="eu-north-1"

echo "Building Lambda Package..."

rm -rf package
mkdir package

pip install -r aws/requirements-lambda.txt -t package
cp aws/lambda_handler.py package/

cd package
zip -r lambda.zip . > /dev/null
cd ..

echo "Deploying CloudFormation..."

read -p "S3 Bucket Name: " S3_BUCKET
read -p "RDS Host: " PG_HOST

read -p "Database Name [chatbot]: " PG_DB
PG_DB=${PG_DB:-chatbot}

read -p "Database User [postgres]: " PG_USER
PG_USER=${PG_USER:-postgres}

read -s -p "Database Password: " PG_PASSWORD
echo ""

aws cloudformation deploy \
  --template-file aws/cloudformation.yml \
  --stack-name chatbot-lambda \
  --capabilities CAPABILITY_NAMED_IAM \
  --region $REGION \
  --parameter-overrides \
    S3BucketName=$S3_BUCKET \
    PgHost=$PG_HOST \
    PgDatabase=$PG_DB \
    PgUser=$PG_USER \
    PgPassword="$PG_PASSWORD"

echo "Deployment Complete"

aws cloudformation describe-stacks \
  --stack-name chatbot-lambda \
  --region $REGION \
  --query "Stacks[0].Outputs"