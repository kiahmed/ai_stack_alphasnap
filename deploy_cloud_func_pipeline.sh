#!/bin/bash
# deploy_cloud_func_pipeline.sh
# Sets up: Cloud Function (2nd gen, HTTP) + Cloud Scheduler → function URL
# Cloud Scheduler fires HTTP POST to the function, which calls streamQuery
# on the Agent Engine. The function runs up to 60 min on Cloud Run regardless
# of whether the scheduler's connection drops.
#
# Idempotent: safe to re-run. Creates or updates all resources.

set -e

# Re-read ae_config.config bypassing any WSL /mnt/c page-cache staleness.
# `source` on /mnt/c can serve cached bytes when the file was just written
# from Windows (deploy_agent.py / VS Code). Parse via grep on a fresh open.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG_FILE="$SCRIPT_DIR/ae_config.config"
source "$CONFIG_FILE"
ENGINE_ID=$(grep -E '^ENGINE_ID=' "$CONFIG_FILE" | tail -1 | sed -E 's/^ENGINE_ID="?([^"]*)"?$/\1/')

# ==========================================
# Config
# ==========================================
FUNCTION_NAME="market-sweep-runner"
FUNCTION_DIR="./cloud_function"
FUNCTION_TIMEOUT="3540s"   # 59 minutes (Cloud Function 2nd gen max = 3600s)
FUNCTION_MEMORY="512Mi"

# ==========================================
# Helper: verify ENGINE_ID actually exists in Vertex AI before we deploy.
# Catches both stale config (deploy_agent.py write failed) and stale shell
# reads (WSL /mnt/c cache). Fails loud with the offending ID + a list of
# what's actually live, instead of silently deploying a dead pointer.
# ==========================================
verify_engine() {
    echo "Verifying engine $ENGINE_ID exists in $PROJECT_ID/$LOCATION..."
    local TOKEN API_HOST URL HTTP_CODE BODY
    TOKEN=$(gcloud auth print-access-token 2>/dev/null)
    if [ -z "$TOKEN" ]; then
        echo "Error: could not get gcloud access token. Run 'gcloud auth login'."
        exit 1
    fi
    API_HOST="${LOCATION}-aiplatform.googleapis.com"
    URL="https://${API_HOST}/v1/projects/${PROJECT_ID}/locations/${LOCATION}/reasoningEngines/${ENGINE_ID}"
    HTTP_CODE=$(curl -sS -o /tmp/verify_engine.json -w "%{http_code}" \
        -H "Authorization: Bearer $TOKEN" \
        -H "Content-Type: application/json" \
        "$URL")
    if [ "$HTTP_CODE" != "200" ]; then
        echo ""
        echo "Error: ENGINE_ID '$ENGINE_ID' not found (HTTP $HTTP_CODE)."
        echo "       Either the config is stale, or the shell read a cached"
        echo "       copy of ae_config.config (WSL /mnt/c page cache)."
        echo ""
        echo "API response:"
        cat /tmp/verify_engine.json 2>/dev/null || true
        echo ""
        echo "Live engines in $PROJECT_ID/$LOCATION:"
        curl -sS -H "Authorization: Bearer $TOKEN" \
            "https://${API_HOST}/v1/projects/${PROJECT_ID}/locations/${LOCATION}/reasoningEngines" \
            2>&1 || true
        echo ""
        echo "Fix: confirm ENGINE_ID in $CONFIG_FILE matches a live engine, then re-run."
        exit 1
    fi
    local DISPLAY
    DISPLAY=$(grep -o '"displayName"[[:space:]]*:[[:space:]]*"[^"]*"' /tmp/verify_engine.json | head -1 | sed -E 's/.*"([^"]*)"$/\1/')
    echo "  Engine OK: $ENGINE_ID ($DISPLAY)"
}

# ==========================================
# --patch: Update env vars only (no rebuild)
# ==========================================
if [ "$1" == "--patch" ]; then
    echo "============================================"
    echo "  Patching env vars (no rebuild)"
    echo "============================================"

    if [ -z "$ENGINE_ID" ] || [ "$ENGINE_ID" == '""' ]; then
        echo "Error: ENGINE_ID is not set in ae_config.config."
        exit 1
    fi

    verify_engine

    echo "Updating Cloud Run service '$FUNCTION_NAME' with:"
    echo "  PROJECT_ID=$PROJECT_ID"
    echo "  LOCATION=$LOCATION"
    echo "  ENGINE_ID=$ENGINE_ID"

    gcloud run services update "$FUNCTION_NAME" \
        --region="$LOCATION" \
        --project="$PROJECT_ID" \
        --update-env-vars="PROJECT_ID=$PROJECT_ID,LOCATION=$LOCATION,ENGINE_ID=$ENGINE_ID"

    echo ""
    echo "Patched. New revision will be live in seconds."
    echo "To verify: gcloud run services describe $FUNCTION_NAME --region=$LOCATION --project=$PROJECT_ID --format='value(spec.template.spec.containers[0].env)'"
    exit 0
fi

echo "============================================"
echo "  Deploying Cloud Function + Scheduler"
echo "============================================"

if [ -z "$ENGINE_ID" ] || [ "$ENGINE_ID" == '""' ]; then
    echo "Error: ENGINE_ID is not set in ae_config.config. Please run deploy_agent.py first."
    exit 1
fi

verify_engine

# ==========================================
# 1. Deploy Cloud Function (2nd gen, HTTP trigger)
# ==========================================
echo ""
echo "--- Step 1: Cloud Function ---"
echo "Deploying '$FUNCTION_NAME' from $FUNCTION_DIR..."
echo "  PROJECT_ID=$PROJECT_ID"
echo "  LOCATION=$LOCATION"
echo "  ENGINE_ID=$ENGINE_ID"

gcloud functions deploy "$FUNCTION_NAME" \
    --gen2 \
    --region="$LOCATION" \
    --runtime=python312 \
    --source="$FUNCTION_DIR" \
    --entry-point=run_sweep \
    --trigger-http \
    --timeout="$FUNCTION_TIMEOUT" \
    --memory="$FUNCTION_MEMORY" \
    --max-instances=1 \
    --concurrency=1 \
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
echo "Concurrency safety:"
echo "  - Scheduler: max-retry-attempts=0 (no automatic re-fire on flake)"
echo "  - Cloud Run: max-instances=1 + concurrency=1 (second caller gets 429)"
echo "  This guarantees at most one sweep running at a time — protects the"
echo "  entry_id counter and shard writes from cross-invocation races."
echo ""
echo "The function keeps running on Cloud Run even after scheduler's"
echo "30-min attempt-deadline expires."
echo ""
echo "To manually trigger:"
echo "  gcloud scheduler jobs run \"$SCHEDULER_NAME\" --location=$LOCATION"
echo ""
