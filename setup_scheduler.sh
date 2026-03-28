#!/bin/bash

# Read variables from config file
source ae_config.config

if [ -z "$ENGINE_ID" ] || [ "$ENGINE_ID" == '""' ]; then
    echo "❌ Error: ENGINE_ID is not set in ae_config.config. Please run deploy_agent.py first."
    exit 1
fi

ENDPOINT_URI="https://${LOCATION}-aiplatform.googleapis.com/v1beta1/projects/${PROJECT_ID}/locations/${LOCATION}/reasoningEngines/${ENGINE_ID}:query"

echo "Checking if Cloud Scheduler job '$SCHEDULER_NAME' already exists..."

if gcloud scheduler jobs describe "$SCHEDULER_NAME" --location="$LOCATION" > /dev/null 2>&1; then
    echo "Job already exists. Updating with new Engine ID..."
    OUTPUT=$(gcloud scheduler jobs update http "$SCHEDULER_NAME" \
        --location="$LOCATION" \
        --uri="$ENDPOINT_URI" \
        --attempt-deadline=30m \
        --max-retry-attempts=2 \
        --min-backoff=5m \
        --max-backoff=20m \
        --max-retry-duration=1h \
        --message-body='{"class_method": "trigger", "input": {"user_id": "cron_scheduler", "message": "Execute the daily market sweep. Gather findings from scouts, log them, and print the tabular report."}}' \
        --oauth-service-account-email="$SA_EMAIL" 2>&1)
    EXIT_CODE=$?
    ACTION="updated"
else
    echo "Creating new Cloud Scheduler job to run at $TIMEZONE ($SCHEDULE_INTERVAL)..."
    # Capture BOTH the output (stdout + stderr) and the exit code
    OUTPUT=$(gcloud scheduler jobs create http "$SCHEDULER_NAME" \
        --location="$LOCATION" \
        --schedule="$SCHEDULE_INTERVAL" \
        --time-zone="$TIMEZONE" \
        --uri="$ENDPOINT_URI" \
        --attempt-deadline=30m \
        --max-retry-attempts=2 \
        --min-backoff=5m \
        --max-backoff=20m \
        --max-retry-duration=1h \
        --http-method=POST \
        --message-body='{"class_method": "trigger", "input": {"user_id": "cron_scheduler", "message": "Execute the daily market sweep. Gather findings from scouts, log them, and print the tabular report."}}' \
        --oauth-service-account-email="$SA_EMAIL" \
        --headers="Content-Type=application/json" \
        --project="$PROJECT_ID" 2>&1)
    EXIT_CODE=$?
    ACTION="created"
fi

if [ $EXIT_CODE -eq 0 ]; then
    # This block ONLY runs if the Google Cloud command succeeds (Exit Code 0)
    echo "✅ Scheduler job successfully $ACTION!"
    echo "--------------------------------------------------------"
    echo "🛠️ To MANUALLY TRIGGER the agent at any time via CLI, run:"
    echo "gcloud scheduler jobs run \"$SCHEDULER_NAME\" --location=$LOCATION"
    echo "--------------------------------------------------------"
    echo "🌐 API ENDPOINT for external applications (e.g., curl/Postman):"
    echo "POST: $ENDPOINT_URI"
    echo "Requires Bearer Token Auth: \$(gcloud auth print-access-token)"
else
    # This block runs if the Google Cloud command fails
    echo "❌ CRITICAL ERROR: Failed to create the Cloud Scheduler job."
    echo "⚠️ Exit Code: $EXIT_CODE"
    echo "🛑 Exact Error Details from Google Cloud:"
    echo "--------------------------------------------------------"
    echo "$OUTPUT"
    echo "--------------------------------------------------------"
    echo "💡 Hint: If the error mentions 'iam.serviceAccounts.actAs', you need to give your Service Account the 'Service Account User' role."
    exit $EXIT_CODE
fi
# Make script executable
chmod +x setup_scheduler.sh
