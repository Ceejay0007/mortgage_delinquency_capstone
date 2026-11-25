#!/usr/bin/env bash
# Create the AWS Glue job that builds the Freddie Mac Silver tables.
set -euo pipefail
export AWS_PAGER=""

JOB="freddie_mac_silver"
REGION="us-east-2"
BUCKET="conrad-mortgage-capstone-2026"
RELEASE="2026Q2"
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
ROLE_ARN="arn:aws:iam::${ACCOUNT}:role/capstone-glue-role"

# Changes from FRED:
# 1) 5 workers instead of 2: about 56 million performance rows
# 2) 45 minute timeout instead of 10
# 3) --RELEASE is passed, so a new quarter is a setting, not a code change
# 4) --extra files copies the column layout next to the script, where load_column_names() looks first
aws glue create-job \
  --region "$REGION" \
  --name "$JOB" \
  --role "$ROLE_ARN" \
  --glue-version "5.0" \
  --worker-type G.1X \
  --number-of-workers 5 \
  --timeout 45 \
  --max-retries 0 \
  --command "Name=glueetl,ScriptLocation=s3://${BUCKET}/scripts/glue/freddie_mac_silver.py,PythonVersion=3" \
    --default-arguments "{\"--BUCKET\":\"${BUCKET}\",\"--RELEASE\":\"${RELEASE}\",\"--extra-files\":\"s3://${BUCKET}/scripts/config/freddie_mac_columns.yaml\",\"--additional-python-modules\":\"pyyaml==6.0.2\",\"--job-bookmark-option\":\"job-bookmark-disable\"}"

echo "Created Glue job: $JOB"