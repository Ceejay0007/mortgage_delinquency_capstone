#!/usr/bin/env bash
# Create the AWS Glue job that builds the FRED Silver table.
set -euo pipefail
export AWS_PAGER=""

JOB="fred_silver"
REGION="us-east-2"
BUCKET="conrad-mortgage-capstone-2026"
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
ROLE_ARN="arn:aws:iam::${ACCOUNT}:role/capstone-glue-role"

aws glue create-job \
  --region "$REGION" \
  --name "$JOB" \
  --role "$ROLE_ARN" \
  --glue-version "5.0" \
  --worker-type G.1X \
  --number-of-workers 2 \
  --timeout 10 \
  --max-retries 0 \
  --command "Name=glueetl,ScriptLocation=s3://${BUCKET}/scripts/glue/fred_silver.py,PythonVersion=3" \
  --default-arguments "{\"--BUCKET\":\"${BUCKET}\",\"--job-bookmark-option\":\"job-bookmark-disable\"}"

echo "Created Glue job: $JOB"