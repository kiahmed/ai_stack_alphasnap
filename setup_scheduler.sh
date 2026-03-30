#!/bin/bash

# Read variables from config file
source ae_config.config

FUNCTION_NAME="market-sweep-runner"

# Get the Cloud Function URL
FUNCTION_URL=$(gcloud functions describe "$FUNCTION_NAME" \
    --gen2 \
    --region="$LOCATION" \
    --project="$PROJECT_ID" \
    --format="value(serviceConfig.uri)" 2>/dev/null)

if [ -z "$FUNCTION_URL" ]; then
    echo "Error: Cloud Function '$FUNCTION_NAME' not found. Run deploy_pubsub_pipeline.sh first."
    exit 1
fi

echo "Checking if Cloud Scheduler job '$SCHEDULER_NAME' already exists..."

if gcloud scheduler jobs describe "$SCHEDULER_NAME" --location="$LOCATION" --project="$PROJECT_ID" > /dev/null 2>&1; then
    echo "Job already exists. Deleting to recreate..."
    gcloud scheduler jobs delete "$SCHEDULER_NAME" --location="$LOCATION" --project="$PROJECT_ID" --quiet
fi

echo "Creating HTTP scheduler job to run at $TIMEZONE ($SCHEDULE_INTERVAL)..."
OUTPUT=$(gcloud scheduler jobs create http "$SCHEDULER_NAME" \
    --location="$LOCATION" \
    --schedule="$SCHEDULE_INTERVAL" \
    --time-zone="$TIMEZONE" \
    --uri="$FUNCTION_URL" \
    --http-method=POST \
    --attempt-deadline=30m \
    --max-retry-attempts=0 \
    --oidc-service-account-email="$SA_EMAIL" \
    --project="$PROJECT_ID" 2>&1)
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "Scheduler job created!"
    echo "--------------------------------------------------------"
    echo "Target: $FUNCTION_URL"
    echo "Schedule: $SCHEDULE_INTERVAL ($TIMEZONE)"
    echo "Retries: 0 (prevents duplicate sweeps)"
    echo ""
    echo "To manually trigger:"
    echo "  gcloud scheduler jobs run \"$SCHEDULER_NAME\" --location=$LOCATION"
    echo "--------------------------------------------------------"
else
    echo "CRITICAL ERROR: Failed to create the Cloud Scheduler job."
    echo "Exit Code: $EXIT_CODE"
    echo "--------------------------------------------------------"
    echo "$OUTPUT"
    echo "--------------------------------------------------------"
    exit $EXIT_CODE
fi
chmod +x setup_scheduler.sh
