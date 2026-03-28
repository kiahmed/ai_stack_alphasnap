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
#  Fire-and-forget: calls the trigger method (returns immediately,
#  sweep runs in background on the Reasoning Engine).
#  Also supports "api_stream" for synchronous streaming output.
# ═══════════════════════════════════════════════════
elif [ "$TRIGGER_MODE" == "api" ]; then
    echo "🔗 Triggering via fire-and-forget API call to Reasoning Engine ${ENGINE_ID}..."

    python3 - <<EOF
import requests, json, sys

url = "https://${LOCATION}-aiplatform.googleapis.com/v1beta1/projects/${PROJECT_ID}/locations/${LOCATION}/reasoningEngines/${ENGINE_ID}:query"
headers = {
    "Authorization": "Bearer $(gcloud auth print-access-token)",
    "Content-Type": "application/json"
}
payload = {
    "class_method": "trigger",
    "input": {
        "user_id": "api_trigger_cli",
        "message": "Execute the daily market sweep. Gather findings from scouts, log them, and print the tabular report."
    }
}

print("📡 Sending trigger request...")
try:
    r = requests.post(url, headers=headers, json=payload, timeout=120)
except requests.exceptions.ConnectionError as e:
    print(f"❌ CONNECTION ERROR: Could not reach the Reasoning Engine.")
    print(f"   Check that ENGINE_ID={payload.get('engine_id', 'N/A')} is correct and the engine is deployed.")
    print(f"   Detail: {e}")
    sys.exit(1)
except requests.exceptions.Timeout:
    print(f"❌ TIMEOUT: Request took longer than 120s. The engine may be cold-starting.")
    print(f"   Try again in a minute, or check Cloud Logging for startup errors.")
    sys.exit(1)

print(f"📨 Response status: {r.status_code}")
print(f"📦 Response body:   {r.text[:500]}")

if r.status_code == 200:
    try:
        body = r.json()
        # Reasoning Engine wraps response in an "output" key
        output = body if isinstance(body, dict) and "status" in body else body.get("output", body)
        status = output.get("status", "unknown") if isinstance(output, dict) else str(output)
        print(f"\n✅ Trigger response: {status}")
        if isinstance(output, dict) and output.get("message"):
            print(f"   {output['message']}")
        print("\n💡 The sweep is now running in the background on the Reasoning Engine.")
        print("   Monitor progress via Cloud Logging:")
        print(f'   gcloud logging read "resource.type=aiplatform.googleapis.com/ReasoningEngine AND resource.labels.location=${LOCATION}" --limit=50 --project=${PROJECT_ID} --format="value(textPayload)"')
    except json.JSONDecodeError:
        print(f"\n✅ Trigger sent successfully (non-JSON response).")
elif r.status_code == 404:
    print(f"\n❌ ENGINE NOT FOUND (404)")
    print(f"   ENGINE_ID '${ENGINE_ID}' does not exist or was deleted.")
    print(f"   Run deploy_agent.py to create a new engine, then update ae_config.config.")
elif r.status_code == 401 or r.status_code == 403:
    print(f"\n❌ AUTH ERROR ({r.status_code})")
    print(f"   Your access token may be expired or the service account lacks permissions.")
    print(f"   Run: gcloud auth print-access-token  (to verify)")
elif r.status_code == 400:
    print(f"\n❌ BAD REQUEST (400)")
    print(f"   The engine rejected the request. This usually means the 'trigger' method")
    print(f"   is not registered on the deployed app. Redeploy with: python3 deploy_agent.py")
    try:
        detail = r.json()
        print(f"   Detail: {json.dumps(detail, indent=2)[:300]}")
    except:
        pass
else:
    print(f"\n❌ UNEXPECTED ERROR ({r.status_code})")
    print(f"   Response: {r.text[:500]}")

sys.exit(0 if r.status_code == 200 else 1)
EOF

# ═══════════════════════════════════════════════════
#  TRIGGER_MODE = "api_stream"
#  Synchronous streaming — waits for full output (may timeout after 30 min)
# ═══════════════════════════════════════════════════
elif [ "$TRIGGER_MODE" == "api_stream" ]; then
    echo "🔗 Streaming via direct API call to Reasoning Engine ${ENGINE_ID}..."

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

try:
    with requests.post(url, headers=headers, json=payload, stream=True, timeout=3600) as r:
        if r.status_code != 200:
            print(f"❌ Error: {r.status_code}")
            print(f"   {r.text[:500]}")
            sys.exit(1)

        for line in r.iter_lines():
            if not line: continue
            try:
                event = json.loads(line.decode('utf-8'))
                if "usage_metadata" in event:
                    u = event["usage_metadata"]
                    total_tokens["input"] = u.get("promptTokenCount", 0)
                    total_tokens["output"] = u.get("candidatesTokenCount", 0)
                    total_tokens["total"] = u.get("totalTokenCount", 0)
                if "content" in event:
                    print(event["content"], end="", flush=True)
            except json.JSONDecodeError:
                pass
except requests.exceptions.ConnectionError as e:
    print(f"❌ CONNECTION ERROR: {e}")
    sys.exit(1)
except requests.exceptions.Timeout:
    print(f"❌ TIMEOUT: Stream exceeded 1 hour.")
    sys.exit(1)

print("\n" + "="*40)
print("📈 FINAL SWEEP METRICS (Sequential Aggregation)")
print("="*40)
print(f"🔹 Total Input:  {total_tokens['input']:,}")
print(f"🔹 Total Output: {total_tokens['output']:,}")
print(f"🔹 Total Sweep Tokens: {total_tokens['total']:,}")
print("="*40 + "\n")
EOF

# ═══════════════════════════════════════════════════
#  TRIGGER_MODE = "kill"
#  Sends a kill signal to stop a running sweep
# ═══════════════════════════════════════════════════
elif [ "$TRIGGER_MODE" == "kill" ]; then
    echo "🛑 Sending KILL signal to Reasoning Engine ${ENGINE_ID}..."

    python3 - <<EOF
import requests, json, sys

url = "https://${LOCATION}-aiplatform.googleapis.com/v1beta1/projects/${PROJECT_ID}/locations/${LOCATION}/reasoningEngines/${ENGINE_ID}:query"
headers = {
    "Authorization": "Bearer $(gcloud auth print-access-token)",
    "Content-Type": "application/json"
}
payload = {
    "class_method": "kill",
    "input": {}
}

try:
    r = requests.post(url, headers=headers, json=payload, timeout=30)
except requests.exceptions.ConnectionError as e:
    print(f"❌ CONNECTION ERROR: {e}")
    sys.exit(1)
except requests.exceptions.Timeout:
    print(f"❌ TIMEOUT: Engine did not respond within 30s.")
    sys.exit(1)

print(f"📨 Response status: {r.status_code}")

if r.status_code == 200:
    try:
        body = r.json()
        output = body if isinstance(body, dict) and "status" in body else body.get("output", body)
        status = output.get("status", "unknown") if isinstance(output, dict) else str(output)
        message = output.get("message", "") if isinstance(output, dict) else ""
        print(f"\n{'🛑' if 'kill' in status else '💤'} {status}: {message}")
    except json.JSONDecodeError:
        print(f"\n📦 Raw response: {r.text[:500]}")
else:
    print(f"\n❌ Error ({r.status_code}): {r.text[:500]}")

sys.exit(0 if r.status_code == 200 else 1)
EOF

# ═══════════════════════════════════════════════════
#  TRIGGER_MODE = "status"
#  Checks if a sweep is currently running
# ═══════════════════════════════════════════════════
elif [ "$TRIGGER_MODE" == "status" ]; then
    echo "🔍 Checking sweep status on Reasoning Engine ${ENGINE_ID}..."

    python3 - <<EOF
import requests, json, sys

url = "https://${LOCATION}-aiplatform.googleapis.com/v1beta1/projects/${PROJECT_ID}/locations/${LOCATION}/reasoningEngines/${ENGINE_ID}:query"
headers = {
    "Authorization": "Bearer $(gcloud auth print-access-token)",
    "Content-Type": "application/json"
}
payload = {
    "class_method": "status",
    "input": {}
}

try:
    r = requests.post(url, headers=headers, json=payload, timeout=30)
except requests.exceptions.ConnectionError as e:
    print(f"❌ CONNECTION ERROR: {e}")
    sys.exit(1)
except requests.exceptions.Timeout:
    print(f"❌ TIMEOUT: Engine did not respond within 30s.")
    sys.exit(1)

print(f"📨 Response status: {r.status_code}")

if r.status_code == 200:
    try:
        body = r.json()
        output = body if isinstance(body, dict) and "status" in body else body.get("output", body)
        status = output.get("status", "unknown") if isinstance(output, dict) else str(output)
        if status == "running":
            started = output.get("started_at", "?") if isinstance(output, dict) else "?"
            print(f"\n🔄 Sweep is RUNNING (started at {started})")
        else:
            print(f"\n💤 Engine is IDLE — no sweep in progress.")
    except json.JSONDecodeError:
        print(f"\n📦 Raw response: {r.text[:500]}")
else:
    print(f"\n❌ Error ({r.status_code}): {r.text[:500]}")

sys.exit(0 if r.status_code == 200 else 1)
EOF

else
    echo "❌ Error: Invalid TRIGGER_MODE='${TRIGGER_MODE}' in ae_config.config."
    echo "   Valid options: 'api', 'api_stream', 'scheduler', 'kill', or 'status'"
    exit 1
fi