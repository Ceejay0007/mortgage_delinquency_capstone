#!/usr/bin/env bash
# Create the IAM role that AWS Glue jobs run under.
# Run from the folder containing the two JSON policy files.
set -euo pipefail
export AWS_PAGER=""   # print output directly instead of opening a pager

ROLE="capstone-glue-role"

# 1) Create the role, trusted only by the Glue service
aws iam create-role \
  --role-name "$ROLE" \
  --assume-role-policy-document file://glue_trust_policy.json

# 2) Attach AWS's managed policy for Glue basics (logs, Data Catalog)
aws iam attach-role-policy \
  --role-name "$ROLE" \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

# 3) Add our bucket permissions (least privilege)
aws iam put-role-policy \
  --role-name "$ROLE" \
  --policy-name capstone-glue-s3 \
  --policy-document file://glue_role_s3_policy.json

# 4) Show the result
echo "Managed policies:"
aws iam list-attached-role-policies --role-name "$ROLE"
echo "Inline policies:"
aws iam list-role-policies --role-name "$ROLE"