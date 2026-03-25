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

    # ── Job State Check & Auto-Resume ──
    echo "🔍 Checking job state for $SCHEDULER_NAME..."
    STATE=$(gcloud scheduler jobs describe "$SCHEDULER_NAME" \
        --location="$LOCATION" \
        --project="$PROJECT_ID" \
        --format="value(state)" 2>/dev/null)
    
    if [ "$STATE" == "PAUSED" ]; then
        echo "⚠️ Job is PAUSED. Resuming now..."
        gcloud scheduler jobs resume "$SCHEDULER_NAME" \
            --location="$LOCATION" \
            --project="$PROJECT_ID" > /dev/null
        echo "✅ Job resumed."
    elif [ -z "$STATE" ]; then
        echo "❌ Error: Job '$SCHEDULER_NAME' not found in $LOCATION."
        exit 1
    fi

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
    
    # We use a python one-liner to handle the stream, print deltas, and accumulate usage_metadata.
    # This captures the native SequentialAgent aggregation for the entire sweep.
    python3 - <<EOF
import requests, json, sys

url = "https://${LOCATION}-aiplatform.googleapis.com/v1beta1/projects/${PROJECT_ID}/locations/${LOCATION}/reasoningEngines/${ENGINE_ID}:streamQuery"
headers = {
    "Authorization": "Bearer $(gcloud auth print-access-token)",
    "Content-Type": "application/json"
}
payload = {
    "class_method": "stream_query",
    "input": {
        "user_id": "api_trigger_cli",
        "message": "Execute the daily market sweep. Gather findings from scouts, log them, and print the tabular report."
    }
}

print("💡 Streaming results from Vertex AI...")
total_tokens = {"input": 0, "output": 0, "total": 0}

with requests.post(url, headers=headers, json=payload, stream=True) as r:
    if r.status_code != 200:
        print(f"❌ Error: {r.status_code}\n{r.text}")
        sys.exit(1)
    
    for line in r.iter_lines():
        if not line: continue
        try:
            event = json.loads(line.decode('utf-8'))
            # SequentialAgent Aggregation: Vertex tracks the sum of all steps.
            # We look for usage_metadata in the events.
            if "usage_metadata" in event:
                u = event["usage_metadata"]
                total_tokens["input"] = u.get("promptTokenCount", 0)
                total_tokens["output"] = u.get("candidatesTokenCount", 0)
                total_tokens["total"] = u.get("totalTokenCount", 0)
            
            # Print text content if present
            if "content" in event:
                print(event["content"], end="", flush=True)
            else:
                # Print other event names (tool calls, etc)
                pass
        except:
            pass

print("\n" + "="*40)
print("📈 FINAL SWEEP METRICS (Sequential Aggregation)")
print("="*40)
print(f"🔹 Total Input:  {total_tokens['input']:,}")
print(f"🔹 Total Output: {total_tokens['output']:,}")
print(f"🔹 Total Sweep Tokens: {total_tokens['total']:,}")
print("="*40 + "\n")
EOF

else
    echo "❌ Error: Invalid TRIGGER_MODE='${TRIGGER_MODE}' in ae_config.config."
    echo "   Valid options: 'api' or 'scheduler'"
    exit 1
fi