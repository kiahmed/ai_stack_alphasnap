

# --- AUTOMATION BLOCK ---
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SVC_ACCOUNT="$SCRIPT_DIR/service_account.json"

if [ -f "$SVC_ACCOUNT" ]; then
    echo "✅ Service Account found. Setting up headless authentication..."
    export GOOGLE_APPLICATION_CREDENTIALS="$SVC_ACCOUNT"
else
    echo "⚠️  No service_account.json found. Will try to auto create the session with an existing service account "
    "$SCRIPT_DIR/create_svc_accounts.sh"
    echo " If still fails, wil fall back to interactive ADC (User Credentials).  If your session expires, run: gcloud auth application-default login"
fi

# --- STORAGE & LOGS ---
#You can revert to Python 3.10 anytime with sudo update-alternatives --set python3 /usr/bin/python3.10.
# gsutil signurl -d 12h -u gs://marketresearch-agents/market_findings_log.json
# gsutil acl ch -u AllUsers:R gs://marketresearch-agents/market_findings_log.json
# gsutil acl ch -d AllUsers gs://marketresearch-agents/market_findings_log.json
#Sign the URL for the log file
# gcloud storage sign-url gs://marketresearch-agents/market_findings_log.json \
#   --duration=24h \
#   --region=us-central1
#   https://storage.cloud.google.com/marketresearch-agents/market_findings_log.json?authuser=1

# SET THE QUOTA PROJECT
# gcloud auth application-default set-quota-project marketresearch-agents  
#GIVE PERMISSION TO THE SERVICE ACCOUNT TO RUN THE SCHEDULER
#gcloud projects add-iam-policy-binding marketresearch-agents \
#    --member="serviceAccount:market-agent-sa@marketresearch-agents.iam.gserviceaccount.com" \
#    --role="roles/cloudscheduler.admin"
#UPDATE THE SCHEDULER
# gcloud scheduler jobs update http market-team-daily-sweep \
#     --location="us-central1" \
#     --uri="https://us-central1-aiplatform.googleapis.com/v1beta1/projects/marketresearch-agents/locations/us-central1/reasoningEngines/8053420196645306368:streamQuery" \
#     --message-body='{"class_method": "stream_query", "input": {"user_id": "cron_scheduler", "message": "Execute the daily market sweep. Gather findings from scouts, log them, and print the tabular report."}}'
# List enabled services for the project
#gcloud services list --enabled --project marketresearch-agents
#get logging from gcloud
# gcloud logging read "resource.type=aiplatform.googleapis.com/ReasoningEngine AND resource.labels.location=us-central1" --limit=50 --project=marketresearch-agents --format="value(textPayload)"
# gcloud logging read "resource.type=aiplatform.googleapis.com/ReasoningEngine AND resource.labels.location=us-central1 AND resource.labels.reasoning_engine_id="3968674576073752576" AND timestamp>="2026-03-30T00:00:00Z"' --limit=5000 --project=marketresearch-agents --format="value(textPayload)" > output.log
#Get individual TOKEN_USAGE entries from yesterday
# gcloud logging read 'resource.type="aiplatform.googleapis.com/ReasoningEngine" AND resource.labels.location="us-central1" AND      
#   timestamp>="2026-03-29T00:00:00Z" AND timestamp<"2026-03-30T00:00:00Z" AND textPayload:"TOKEN_USAGE"' --limit=500                  
#   --project=marketresearch-agents --format="value(textPayload,resource.labels.reasoning_engine_id)" 2>&1 | head -60 
# Filter by LQL 
# resource.type="aiplatform.googleapis.com/ReasoningEngine"
# resource.labels.location="us-central1"
# timestamp >= "2026-03-20T00:00:00Z"
# textPayload:"ERROR"
# severity="DEFAULT"

#gcloud auth list //shows all auth users that were configured
#gcloud config list //shows the active user and project
#GIVE PERMISSION TO THE SERVICE ACCOUNT TO USE THE API
# gcloud projects add-iam-policy-binding marketresearch-agents \
#     --member="serviceAccount:market-agent-sa@marketresearch-agents.iam.gserviceaccount.com" \
#     --role="roles/serviceusage.serviceUsageConsumer"
#
# INCREASE THE TIMEOUT FOR THE SCHEDULER
# gcloud scheduler jobs update http market-team-daily-sweep \
#     --location="us-central1" \
#     --attempt-deadline=30m
#
#GIVE PERMISSION TO THE SERVICE ACCOUNT TO USE THE TRACING API
# gcloud projects add-iam-policy-binding marketresearch-agents \
#     --member="serviceAccount:market-agent-sa@marketresearch-agents.iam.gserviceaccount.com" \
#     --role="roles/cloudtrace.agent"

#COPY THE LONG TERM STORAGE FILE TO THE WORKING DIRECTORY
#gcloud storage cp market_findings_log_lts.json gs://marketresearch-agents/market_findings_log.json
# 
#GIVE PERMISSION TO THE SERVICE ACCOUNT TO USE THE LOGGING API
#gcloud projects add-iam-policy-binding marketresearch-agents --member="serviceAccount:market-agent-sa@marketresearch-agents.iam.gserviceaccount.com" --role="roles/logging.viewer"  

# what are you trying to do with --agent switch? I would like to possibly use the mcp tool to get the TICKER DATA on                 
# task_istructions 4 for DE agent to get confirmed data for the ticker for better grounding. Lets take a step by step approach write   
# resuable function modules that uses @test_atlas_mcp.py for mcp tools usage like NET GEX, DEX, top vol, OI ect based on a supplied    
# ticker. Make these functions transparent and mcp uses expandable so I want to replace atlas-mcp with a different underlying mcp      
# server down the road I could just easily change it in a new config file called mcp.config that you'd create holding all mcp related  
# configuration. move the file and mcp testing/server codes creatign a new folder called mcp. Just write those resuable module         
# functions first to test, dont hook up with the market_team.py project yet

#Get individual TOKEN_USAGE entries from yesterday
# gcloud logging read 'resource.type="aiplatform.googleapis.com/ReasoningEngine" AND resource.labels.location="us-central1" AND      
#   timestamp>="2026-03-31T00:00:00Z" AND timestamp<"2026-04-01T00:00:00Z" AND textPayload:"TOKEN_USAGE"' --limit=500                  
#   --project=marketresearch-agents --format="value(textPayload,resource.labels.reasoning_engine_id)" 2>&1 | head -60 
#get logs for a pariticular agent engine from yesterday and output to a file
#gcloud logging read 'resource.type="aiplatform.googleapis.com/ReasoningEngine" AND resource.labels.location="us-central1" AND resource.labels.reasoning_engine_id="9172513576756183040" AND timestamp>="2026-03-31T00:00:00Z"' --limit=5000 --project=marketresearch-agents --format="value(textPayload)" > output.log