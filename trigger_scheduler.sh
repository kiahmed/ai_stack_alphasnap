#!/bin/bash
source ae_config.config

echo "🚀 Trigger Mode: ${TRIGGER_MODE}"
echo "   Engine ID:    ${ENGINE_ID}"
echo "   Project:      ${PROJECT_ID}"
echo "   Location:     ${LOCATION}"
echo "========================================"

# ── Guard: ENGINE_ID must be set for api mode ──
if [ "$TRIGGER_MODE" == "api" ]; then
    if [ -z "$ENGINE_ID" ] || [ "$ENGINE_ID" == '""' ]; then
        echo "❌ Error: ENGINE_ID is not set in ae_config.config. Please run deploy_agent.py first."
        exit 1
    fi
fi

# ═══════════════════════════════════════════════════
#  TRIGGER_MODE = "scheduler"
#  Runs the job via Cloud Scheduler (async, fire-and-forget)
# ═══════════════════════════════════════════════════
if [ "$TRIGGER_MODE" == "scheduler" ]; then
    echo "📅 Triggering via Cloud Scheduler job: $SCHEDULER_NAME..."

    OUTPUT=$(gcloud scheduler jobs run "$SCHEDULER_NAME" \
        --location="$LOCATION" \
        --project="$PROJECT_ID" 2>&1)

    EXIT_CODE=$?

    if [ $EXIT_CODE -eq 0 ]; then
        echo "✅ Trigger successful! The job is now running in the background."
        echo "--------------------------------------------------------"
        echo "⏳ Because Cloud Scheduler is asynchronous, it does not wait for the AI to finish."
        echo "To view your CIO agent's actual output, wait 1-2 minutes and run this logging command:"
        echo ""
        echo "gcloud logging read \"resource.type=aiplatform.googleapis.com/ReasoningEngine AND resource.labels.location=$LOCATION\" --limit=50 --project=$PROJECT_ID --format=\"value(textPayload)\""
        echo "--------------------------------------------------------"
    else
        echo "❌ CRITICAL ERROR: Failed to trigger the scheduler job."
        echo "⚠️ Exit Code: $EXIT_CODE"
        echo "🛑 Exact Error Details from Google Cloud:"
        echo "--------------------------------------------------------"
        echo "$OUTPUT"
        echo "--------------------------------------------------------"

        # Common troubleshooting hints based on the output
        if [[ "$OUTPUT" == *"NOT_FOUND"* ]]; then
            echo "💡 Hint: The job doesn't exist. Did you delete it? You may need to run setup_scheduler.sh again."
        elif [[ "$OUTPUT" == *"FAILED_PRECONDITION"* || "$OUTPUT" == *"PAUSED"* ]]; then
            echo "💡 Hint: The job is paused. Run this command to unpause it, then try again:"
            echo "gcloud scheduler jobs resume $SCHEDULER_NAME --location=$LOCATION"
        fi
        exit $EXIT_CODE
    fi

# ═══════════════════════════════════════════════════
#  TRIGGER_MODE = "api"
#  Calls the Reasoning Engine streamQuery endpoint directly
# ═══════════════════════════════════════════════════
elif [ "$TRIGGER_MODE" == "api" ]; then
    echo "🔗 Triggering via direct API call to Reasoning Engine ${ENGINE_ID}..."

    curl -X POST \
      -H "Authorization: Bearer $(gcloud auth print-access-token)" \
      -H "Content-Type: application/json" \
      "https://${LOCATION}-aiplatform.googleapis.com/v1beta1/projects/${PROJECT_ID}/locations/${LOCATION}/reasoningEngines/${ENGINE_ID}:streamQuery" \
      -d '{
        "class_method": "stream_query",
        "input": {
            "user_id": "api_user",
            "message": "Execute the daily market sweep. Run the CIO to gather findings from scouts, log them, and print the tabular report."
        }
      }'

else
    echo "❌ Error: Invalid TRIGGER_MODE='${TRIGGER_MODE}' in ae_config.config."
    echo "   Valid options: 'api' or 'scheduler'"
    exit 1
fi