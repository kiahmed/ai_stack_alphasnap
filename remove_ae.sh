#!/bin/bash
source ae_config.config

if [ -z "$ENGINE_ID" ] || [ "$ENGINE_ID" == '""' ]; then
    echo "❌ Error: ENGINE_ID is not set in ae_config.config."
    exit 1
fi

curl -X DELETE \
  -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  "https://${LOCATION}-aiplatform.googleapis.com/v1beta1/projects/${PROJECT_ID}/locations/${LOCATION}/reasoningEngines/${ENGINE_ID}?force=true"

# Remove ENGINE_ID from config automatically
sed -i 's/^ENGINE_ID=.*/ENGINE_ID=""/' ae_config.config

# Make script executable
chmod +x remove_ae.sh
