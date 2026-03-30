#!/bin/bash
source ae_config.config

FUNCTION_NAME="market-sweep-runner"

echo "Trigger Mode: ${TRIGGER_MODE}"
echo "   Engine ID:    ${ENGINE_ID}"
echo "   Project:      ${PROJECT_ID}"
echo "   Location:     ${LOCATION}"
echo "========================================"

# ═══════════════════════════════════════════════════
#  TRIGGER_MODE = "scheduler"
#  Triggers via Cloud Scheduler (fire-and-forget).
#  Scheduler calls Cloud Function which calls streamQuery.
# ═══════════════════════════════════════════════════
if [ "$TRIGGER_MODE" == "scheduler" ]; then
    echo "Triggering via Cloud Scheduler job: $SCHEDULER_NAME..."

    # Check job state and auto-resume if paused
    STATE=$(gcloud scheduler jobs describe "$SCHEDULER_NAME" \
        --location="$LOCATION" \
        --project="$PROJECT_ID" \
        --format="value(state)" 2>/dev/null)

    if [ "$STATE" == "PAUSED" ]; then
        echo "Job is PAUSED. Resuming..."
        gcloud scheduler jobs resume "$SCHEDULER_NAME" \
            --location="$LOCATION" \
            --project="$PROJECT_ID" > /dev/null
        echo "Job resumed."
    elif [ -z "$STATE" ]; then
        echo "Error: Job '$SCHEDULER_NAME' not found. Run setup_scheduler.sh first."
        exit 1
    fi

    OUTPUT=$(gcloud scheduler jobs run "$SCHEDULER_NAME" \
        --location="$LOCATION" \
        --project="$PROJECT_ID" 2>&1)
    EXIT_CODE=$?

    if [ $EXIT_CODE -eq 0 ]; then
        echo "Trigger sent! Cloud Function will run the sweep."
        echo "--------------------------------------------------------"
        echo "Monitor via Cloud Function logs:"
        echo "  gcloud functions logs read $FUNCTION_NAME --gen2 --region=$LOCATION --project=$PROJECT_ID --limit=50"
        echo "--------------------------------------------------------"
    else
        echo "Failed to trigger scheduler job."
        echo "$OUTPUT"
        exit $EXIT_CODE
    fi

# ═══════════════════════════════════════════════════
#  TRIGGER_MODE = "api"
#  Direct streamQuery call to the Agent Engine.
#  Holds connection, streams output. For debugging.
# ═══════════════════════════════════════════════════
elif [ "$TRIGGER_MODE" == "api" ]; then
    if [ -z "$ENGINE_ID" ] || [ "$ENGINE_ID" == '""' ]; then
        echo "Error: ENGINE_ID is not set in ae_config.config."
        exit 1
    fi

    echo "Streaming via direct streamQuery to Engine ${ENGINE_ID}..."

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

print("Streaming results from Vertex AI...")

try:
    with requests.post(url, headers=headers, json=payload, stream=True, timeout=3600) as r:
        if r.status_code != 200:
            print(f"Error: {r.status_code}")
            print(f"   {r.text[:500]}")
            sys.exit(1)

        for line in r.iter_lines():
            if not line: continue
            try:
                event = json.loads(line.decode('utf-8'))
                if "content" in event:
                    print(event["content"], end="", flush=True)
            except json.JSONDecodeError:
                pass
except requests.exceptions.ConnectionError as e:
    print(f"CONNECTION ERROR: {e}")
    sys.exit(1)
except requests.exceptions.Timeout:
    print(f"TIMEOUT: Stream exceeded 1 hour.")
    sys.exit(1)

print("\nStream complete.")
EOF

else
    echo "Error: Invalid TRIGGER_MODE='${TRIGGER_MODE}' in ae_config.config."
    echo "   Valid options: 'scheduler' (fire-and-forget via Cloud Function) or 'api' (direct streamQuery)"
    exit 1
fi
