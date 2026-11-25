#!/usr/bin/env bash
# Start any Glue job and wait until it finishes.
# Usage: ./infra/aws/run_glue_job.sh freddie_mac_silver
set -euo pipefail
export AWS_PAGER=""

JOB="${1:?Usage: run_glue_job.sh <job_name>}"
REGION="us-east-2"

RUN_ID=$(aws glue start-job-run --region "$REGION" --job-name "$JOB" --query JobRunId --output text)
echo "Started $JOB run: $RUN_ID"

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
  sleep 30
done