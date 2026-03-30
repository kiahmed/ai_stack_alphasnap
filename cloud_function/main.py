"""Cloud Function (2nd gen, HTTP trigger) → Agent Engine streamQuery bridge.

Called by Cloud Scheduler via HTTP POST. The function holds an active
connection to the Reasoning Engine's streamQuery endpoint for the full
duration of the sweep (~30 min). Cloud Scheduler's 30-min attempt-deadline
may expire, but this function keeps running on Cloud Run for up to 60 min.
"""

import os
import requests
import google.auth
from google.auth.transport.requests import Request

PROJECT_ID = os.environ.get("PROJECT_ID", "marketresearch-agents")
LOCATION = os.environ.get("LOCATION", "us-central1")
ENGINE_ID = os.environ.get("ENGINE_ID")


def run_sweep(request):
    """HTTP Cloud Function entrypoint."""
    if not ENGINE_ID:
        print("FATAL: ENGINE_ID environment variable not set.")
        return "missing ENGINE_ID", 500

    url = (
        f"https://{LOCATION}-aiplatform.googleapis.com/v1beta1/"
        f"projects/{PROJECT_ID}/locations/{LOCATION}/"
        f"reasoningEngines/{ENGINE_ID}:streamQuery"
    )

    creds, _ = google.auth.default()
    creds.refresh(Request())
    headers = {
        "Authorization": f"Bearer {creds.token}",
        "Content-Type": "application/json",
    }
    payload = {
        "class_method": "stream_query",
        "input": {
            "user_id": "cron_scheduler",
            "message": "Execute the daily market sweep. Gather findings from scouts, log them, and print the tabular report.",
        },
    }

    print(f"Calling streamQuery on engine {ENGINE_ID}...")
    try:
        with requests.post(url, headers=headers, json=payload, stream=True, timeout=3300) as r:
            r.raise_for_status()
            for line in r.iter_lines():
                if line:
                    print(line.decode("utf-8", errors="replace"))
        print("Sweep completed successfully.")
        return "done", 200
    except requests.exceptions.Timeout:
        print("ERROR: streamQuery timed out after 55 minutes.")
        return "timeout", 504
    except Exception as e:
        print(f"ERROR: {e}")
        return str(e), 500
