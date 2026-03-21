#!/bin/bash
source ae_config.config

# Store previous account
PREV_ACCOUNT=$SA_EMAIL
# OWNER_ACCOUNT="info@solutionjet.net"

# echo "Switching gcloud identity to $OWNER_ACCOUNT to grant IAM roles..."
# gcloud config set account "$OWNER_ACCOUNT"

echo "Granting AI Platform User..."
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/aiplatform.user" > /dev/null

echo "Granting Storage Object Admin..."
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/storage.objectAdmin" > /dev/null

echo "Granting Logging Log Writer..."
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/logging.logWriter" > /dev/null

echo "Granting Cloud Scheduler Admin..."
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/cloudscheduler.admin" > /dev/null

echo "Granting IAM Service Account User..."
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/iam.serviceAccountUser" > /dev/null

echo "Granting Service Usage Consumer..."
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/serviceusage.serviceUsageConsumer" > /dev/null

echo "Granting Cloud Trace Agent..."
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/cloudtrace.agent" > /dev/null

echo "Restoring previous gcloud identity ($PREV_ACCOUNT)..."
gcloud config set account "$PREV_ACCOUNT"

echo "✅ All IAM roles successfully bound to $SA_EMAIL!"
