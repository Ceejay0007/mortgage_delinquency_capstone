#!/usr/bin/env bash
# Start the FRED Silver Glue job and wait until it finishes.
set -euo pipefail
export AWS_PAGER=""

JOB="fred_silver"
REGION="us-east-2"

RUN_ID=$(aws glue start-job-run --region "$REGION" --job-name "$JOB" --query JobRunId --output text)
echo "Started run: $RUN_ID"

while true; do
  STATE=$(aws glue get-job-run --region "$REGION" --job-name "$JOB" --run-id "$RUN_ID" \
    --query JobRun.JobRunState --output text)
  echo "$(date +%H:%M:%S)  $STATE"
  case "$STATE" in
    SUCCEEDED) echo "Job succeeded."; exit 0 ;;
    FAILED|ERROR|TIMEOUT|STOPPED)
      aws glue get-job-run --region "$REGION" --job-name "$JOB" --run-id "$RUN_ID" \
        --query JobRun.ErrorMessage --output text
      exit 1 ;;
  esac
  sleep 20
done