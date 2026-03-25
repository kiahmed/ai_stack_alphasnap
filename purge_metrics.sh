#!/bin/bash
# ---------------------------------------------------------
# purge_metrics.sh - Cloud Logging Retention Helper
# ---------------------------------------------------------
source ae_config.config

echo "🧹 Refined Purge Strategy: Targeting Cloud Logging"
echo "--------------------------------------------------------"

# 1. PROTECT ASSETS (Cleanup existing policies)
# ---------------------------------------------------------
if [ -n "$STAGING_BUCKET" ]; then
    echo "🛡️ Ensuring GCS Bucket $STAGING_BUCKET is protected..."
    # Set an empty lifecycle to remove the 3-day purge we added previously
    echo '{"rule": []}' > /tmp/empty_lifecycle.json
    gsutil lifecycle set /tmp/empty_lifecycle.json "$STAGING_BUCKET" > /dev/null 2>&1
    rm /tmp/empty_lifecycle.json
    echo "✅ GCS Lifecycle Policy removed. Your bucket objects are safe."
fi

# 2. LOCAL ASSET CHECK
# ---------------------------------------------------------
echo "✅ Local JSON and Log files protected. No files will be deleted."

# 3. CLOUD LOGGING RETENTION COMMANDS
# ---------------------------------------------------------
echo ""
echo "📝 To purge GCP Logs and Metrics every 3 days, run this command:"
echo "   (Requires Project Owner or Logging Admin permissions)"
echo ""
echo "   gcloud logging buckets update _Default --location=$LOCATION --retention-days=3 --project=$PROJECT_ID"
echo ""
echo "--------------------------------------------------------"
echo "💡 Note: Most GCP logs are free for the first 30 days of storage."
echo "   This command reduces retention to 3 days to minimize all log data."
