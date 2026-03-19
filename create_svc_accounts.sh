#gcloud iam service-accounts create market-agent-sa --display-name="Market Agent Service Account"
#gcloud projects add-iam-policy-binding marketresearch-agents --member="serviceAccount:market-agent-sa@marketresearch-agents.iam.gserviceaccount.com" --role="roles/aiplatform.user"
#gcloud projects add-iam-policy-binding marketresearch-agents --member="serviceAccount:market-agent-sa@marketresearch-agents.iam.gserviceaccount.com" --role="roles/storage.admin"
gcloud iam service-accounts keys create service_account.json --iam-account=market-agent-sa@marketresearch-agents.iam.gserviceaccount.com
gcloud auth application-default set-quota-project marketresearch-agents
gcloud auth application-default login