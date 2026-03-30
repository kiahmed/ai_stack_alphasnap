#!/bin/bash
# deploy_cloud_func_pipeline.sh
# Sets up: Cloud Function (2nd gen, HTTP) + Cloud Scheduler → function URL
# Cloud Scheduler fires HTTP POST to the function, which calls streamQuery
# on the Agent Engine. The function runs up to 60 min on Cloud Run regardless
# of whether the scheduler's connection drops.
#
# Idempotent: safe to re-run. Creates or updates all resources.

set -e
source ae_config.config

# ==========================================
# Config
# ==========================================
FUNCTION_NAME="market-sweep-runner"
FUNCTION_DIR="./cloud_function"
FUNCTION_TIMEOUT="3540s"   # 59 minutes (Cloud Function 2nd gen max = 3600s)
FUNCTION_MEMORY="512Mi"

echo "============================================"
echo "  Deploying Cloud Function + Scheduler"
echo "============================================"

if [ -z "$ENGINE_ID" ] || [ "$ENGINE_ID" == '""' ]; then
    echo "Error: ENGINE_ID is not set in ae_config.config. Please run deploy_agent.py first."
    exit 1
fi

# ==========================================
# 1. Deploy Cloud Function (2nd gen, HTTP trigger)
# ==========================================
echo ""
echo "--- Step 1: Cloud Function ---"
echo "Deploying '$FUNCTION_NAME' from $FUNCTION_DIR..."

gcloud functions deploy "$FUNCTION_NAME" \
    --gen2 \
    --region="$LOCATION" \
    --runtime=python312 \
    --source="$FUNCTION_DIR" \
    --entry-point=run_sweep \
    --trigger-http \
    --timeout="$FUNCTION_TIMEOUT" \
    --memory="$FUNCTION_MEMORY" \
    --set-env-vars="PROJECT_ID=$PROJECT_ID,LOCATION=$LOCATION,ENGINE_ID=$ENGINE_ID" \
    --service-account="$SA_EMAIL" \
    --project="$PROJECT_ID" \
    --no-allow-unauthenticated

echo "Cloud Function deployed."

# Grant the service account permission to invoke the function (idempotent)
echo "Ensuring Cloud Run Invoker for $SA_EMAIL..."
gcloud functions add-invoker-policy-binding "$FUNCTION_NAME" \
    --gen2 \
    --region="$LOCATION" \
    --project="$PROJECT_ID" \
    --member="serviceAccount:$SA_EMAIL" --quiet

echo "Invoker permission granted."

# Get the function URL
FUNCTION_URL=$(gcloud functions describe "$FUNCTION_NAME" \
    --gen2 \
    --region="$LOCATION" \
    --project="$PROJECT_ID" \
    --format="value(serviceConfig.uri)" 2>/dev/null)

if [ -z "$FUNCTION_URL" ]; then
    echo "Error: Could not retrieve function URL."
    exit 1
fi

echo "Function URL: $FUNCTION_URL"

# ==========================================
# 2. Update/Create Cloud Scheduler → Cloud Function
# ==========================================
echo ""
echo "--- Step 2: Cloud Scheduler ---"

if gcloud scheduler jobs describe "$SCHEDULER_NAME" --location="$LOCATION" --project="$PROJECT_ID" > /dev/null 2>&1; then
    echo "Updating existing scheduler job '$SCHEDULER_NAME'..."
    gcloud scheduler jobs update http "$SCHEDULER_NAME" \
        --location="$LOCATION" \
        --schedule="$SCHEDULE_INTERVAL" \
        --time-zone="$TIMEZONE" \
        --uri="$FUNCTION_URL" \
        --http-method=POST \
        --attempt-deadline=30m \
        --max-retry-attempts=0 \
        --oidc-service-account-email="$SA_EMAIL" \
        --project="$PROJECT_ID"
    echo "Scheduler job updated."
else
    echo "Creating HTTP scheduler job '$SCHEDULER_NAME'..."
    gcloud scheduler jobs create http "$SCHEDULER_NAME" \
        --location="$LOCATION" \
        --schedule="$SCHEDULE_INTERVAL" \
        --time-zone="$TIMEZONE" \
        --uri="$FUNCTION_URL" \
        --http-method=POST \
        --attempt-deadline=30m \
        --max-retry-attempts=0 \
        --oidc-service-account-email="$SA_EMAIL" \
        --project="$PROJECT_ID"
    echo "Scheduler job created."
fi

# ==========================================
# Done
# ==========================================
echo ""
echo "============================================"
echo "  Pipeline deployed successfully!"
echo "============================================"
echo ""
echo "Flow: Cloud Scheduler ($SCHEDULE_INTERVAL $TIMEZONE)"
echo "   -> Cloud Function: $FUNCTION_URL (59m timeout)"
echo "   -> Agent Engine: $ENGINE_ID (streamQuery)"
echo ""
echo "Scheduler has max-retry-attempts=0 to prevent duplicate sweeps."
echo "The function keeps running on Cloud Run even after scheduler's"
echo "30-min attempt-deadline expires."
echo ""
echo "To manually trigger:"
echo "  gcloud scheduler jobs run \"$SCHEDULER_NAME\" --location=$LOCATION"
echo ""
