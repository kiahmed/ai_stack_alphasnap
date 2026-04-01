#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../ae_config.config"

if [ -z "$ENGINE_ID" ] || [ "$ENGINE_ID" == '""' ]; then
    echo "❌ Error: ENGINE_ID is not set in ae_config.config."
    exit 1
fi

curl -X PATCH \
  -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  -H "Content-Type: application/json" \
  "https://${LOCATION}-aiplatform.googleapis.com/v1beta1/projects/${PROJECT_ID}/locations/${LOCATION}/reasoningEngines/${ENGINE_ID}?updateMask=serviceAccount" \
  -d '{
    "reasoningEngine": {
        "spec": {
                "serviceAccount": "${SA_EMAIL}"
        }
    }
  }'


# Make script executable
chmod +x update_ae.sh


