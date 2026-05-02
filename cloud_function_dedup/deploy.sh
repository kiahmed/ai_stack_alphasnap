#!/usr/bin/env bash
# ============================================================================
# cloud_function_dedup/deploy.sh — deploy + schedule the master-log dedup fn.
#
# Idempotent. Safe to re-run. Reuses the project's existing market-agent SA
# from ae_config.config (no separate SA, no separate IAM bootstrap script).
#
# What this script does:
#   1. Sources ae_config.config + dedup_function.config.
#   2. Verifies / creates the Gmail app-password Secret Manager entry.
#      First run: prompts for the password silently (read -s) and creates the
#      secret. Subsequent runs: confirms it exists and skips.
#   3. Grants the SA the minimum required perms (idempotent):
#        - storage.objectAdmin on the bucket  (read + write master log + backups)
#        - secretmanager.secretAccessor on the gmail-password secret
#        - iam.serviceAccountTokenCreator on itself (so Cloud Scheduler can
#          mint OIDC tokens as it)
#   4. Deploys the Gen 2 Cloud Function with the secret mounted as env var.
#   5. Grants run.invoker on the function to the SA.
#   6. Creates / updates the Cloud Scheduler job.
#
# Usage:
#   bash cloud_function_dedup/deploy.sh
#   bash cloud_function_dedup/deploy.sh --dry-run
#
# After deploy, kick off the first run with:
#   bash cloud_function_dedup/test_fire.sh
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck disable=SC1090
source "$REPO_ROOT/ae_config.config"
# shellcheck disable=SC1090
source "$SCRIPT_DIR/dedup_function.config"

: "${PROJECT_ID:?PROJECT_ID not set in ae_config.config}"
: "${LOCATION:?LOCATION not set in ae_config.config}"
: "${SA_EMAIL:?SA_EMAIL not set in ae_config.config}"

if [[ -t 1 ]]; then
  C_HDR=$'\033[1m'; C_OK=$'\033[0;32m'; C_INFO=$'\033[0;36m'
  C_WARN=$'\033[0;33m'; C_ERR=$'\033[0;31m'; C_OFF=$'\033[0m'
else
  C_HDR=''; C_OK=''; C_INFO=''; C_WARN=''; C_ERR=''; C_OFF=''
fi
header() { echo -e "${C_HDR}$*${C_OFF}"; }
ok()     { echo -e "${C_OK}[OK]${C_OFF}    $*"; }
info()   { echo -e "${C_INFO}[INFO]${C_OFF}  $*"; }
warn()   { echo -e "${C_WARN}[WARN]${C_OFF}  $*"; }
err()    { echo -e "${C_ERR}[ERR]${C_OFF}   $*" >&2; }

DRY_RUN=false
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN=true

header "============================================"
header "  Arboryx — Master-Log Dedup Function Deploy"
header "============================================"
info "Function   : $FUNCTION_NAME"
info "Service AC : $SA_EMAIL"
info "Schedule   : $SCHEDULE_CRON ($SCHEDULE_TZ)"
info "Bucket     : gs://$GCS_BUCKET/$GCS_OBJECT"
info "Email      : $GMAIL_FROM → $GMAIL_TO"
info "Secret     : $GMAIL_SECRET_NAME"
info "Project    : $PROJECT_ID"
info "Region     : $LOCATION"
info "Dry-run    : $DRY_RUN"
echo

# ---------------------------------------------------------------------------
# 1) Gmail app-password secret — create if missing.
# ---------------------------------------------------------------------------
header "--- Step 1: Gmail app-password secret ---"
if gcloud secrets describe "$GMAIL_SECRET_NAME" --project="$PROJECT_ID" >/dev/null 2>&1; then
  ok "Secret '$GMAIL_SECRET_NAME' already exists."
else
  if [[ "$DRY_RUN" == true ]]; then
    info "[dry-run] would prompt for password and create secret '$GMAIL_SECRET_NAME'"
  else
    warn "Secret '$GMAIL_SECRET_NAME' does not exist."
    info "Creating it now. Paste the Gmail app password (input is hidden):"
    read -r -s -p "  password: " GMAIL_APP_PW
    echo
    if [[ -z "$GMAIL_APP_PW" ]]; then
      err "Empty password. Aborting."
      exit 1
    fi
    gcloud secrets create "$GMAIL_SECRET_NAME" \
      --replication-policy="automatic" \
      --project="$PROJECT_ID" >/dev/null
    printf '%s' "$GMAIL_APP_PW" | gcloud secrets versions add "$GMAIL_SECRET_NAME" \
      --data-file=- --project="$PROJECT_ID" >/dev/null
    unset GMAIL_APP_PW
    ok "Secret '$GMAIL_SECRET_NAME' created (v1)."
  fi
fi

# ---------------------------------------------------------------------------
# 2) IAM — grant the existing SA the perms it needs (all idempotent).
# ---------------------------------------------------------------------------
header "--- Step 2: IAM bindings ---"
if [[ "$DRY_RUN" == true ]]; then
  info "[dry-run] would grant storage.objectAdmin on gs://$GCS_BUCKET to $SA_EMAIL"
  info "[dry-run] would grant secretmanager.secretAccessor on $GMAIL_SECRET_NAME"
  info "[dry-run] would grant iam.serviceAccountTokenCreator on $SA_EMAIL (self)"
else
  # Idempotent: gcloud add-iam-policy-binding is a no-op if the exact binding
  # already exists. Use --quiet so the second-run path doesn't print warnings.
  gcloud storage buckets add-iam-policy-binding "gs://$GCS_BUCKET" \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/storage.objectAdmin" \
    --project="$PROJECT_ID" --quiet >/dev/null
  ok "storage.objectAdmin on gs://$GCS_BUCKET → $SA_EMAIL"

  gcloud secrets add-iam-policy-binding "$GMAIL_SECRET_NAME" \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/secretmanager.secretAccessor" \
    --project="$PROJECT_ID" >/dev/null
  ok "secretmanager.secretAccessor on $GMAIL_SECRET_NAME → $SA_EMAIL"

  gcloud iam service-accounts add-iam-policy-binding "$SA_EMAIL" \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/iam.serviceAccountTokenCreator" \
    --project="$PROJECT_ID" --quiet >/dev/null
  ok "iam.serviceAccountTokenCreator on $SA_EMAIL (self)"
fi

# ---------------------------------------------------------------------------
# 3) Deploy the Gen 2 Cloud Function.
# ---------------------------------------------------------------------------
header "--- Step 3: Deploy Cloud Function ---"
ENV_VARS="PROJECT_ID=$PROJECT_ID"
ENV_VARS="$ENV_VARS,GCS_BUCKET=$GCS_BUCKET"
ENV_VARS="$ENV_VARS,GCS_OBJECT=$GCS_OBJECT"
ENV_VARS="$ENV_VARS,BACKUP_PREFIX=$BACKUP_PREFIX"
ENV_VARS="$ENV_VARS,GMAIL_FROM=$GMAIL_FROM"
ENV_VARS="$ENV_VARS,GMAIL_TO=$GMAIL_TO"
ENV_VARS="$ENV_VARS,TFIDF_THRESHOLD=$TFIDF_THRESHOLD"
ENV_VARS="$ENV_VARS,ENTITY_THRESHOLD=$ENTITY_THRESHOLD"
ENV_VARS="$ENV_VARS,NOVELTY_MIN=$NOVELTY_MIN"
ENV_VARS="$ENV_VARS,TFIDF_FLOOR_FOR_ENTITY=$TFIDF_FLOOR_FOR_ENTITY"
ENV_VARS="$ENV_VARS,MEMORY_LIMIT=$MEMORY_LIMIT"
ENV_VARS="$ENV_VARS,RETRY_ATTEMPTS=$RETRY_ATTEMPTS"
ENV_VARS="$ENV_VARS,RETRY_DELAY_SEC=$RETRY_DELAY_SEC"

if [[ "$DRY_RUN" == true ]]; then
  info "[dry-run] would deploy $FUNCTION_NAME with env=$ENV_VARS and secret $GMAIL_SECRET_NAME"
else
  gcloud functions deploy "$FUNCTION_NAME" \
    --gen2 \
    --region="$LOCATION" \
    --runtime=python312 \
    --source="$SCRIPT_DIR" \
    --entry-point=dedup_handler \
    --trigger-http \
    --no-allow-unauthenticated \
    --ingress-settings=all \
    --timeout=540s \
    --memory=512Mi \
    --max-instances=1 \
    --set-env-vars="$ENV_VARS" \
    --set-secrets="GMAIL_APP_PASSWORD=$GMAIL_SECRET_NAME:latest" \
    --service-account="$SA_EMAIL" \
    --project="$PROJECT_ID"
  ok "Function deployed."
fi

# ---------------------------------------------------------------------------
# 4) Get the Cloud Run URL (Gen 2 functions are Cloud Run under the hood).
# ---------------------------------------------------------------------------
FN_URL=""
if [[ "$DRY_RUN" != true ]]; then
  FN_URL=$(gcloud functions describe "$FUNCTION_NAME" \
    --gen2 --region="$LOCATION" --project="$PROJECT_ID" \
    --format='value(serviceConfig.uri)' 2>/dev/null || true)
  if [[ -z "$FN_URL" ]]; then
    err "Could not resolve function URL — deploy probably failed."
    exit 1
  fi
  info "Function URL: $FN_URL"
fi

# ---------------------------------------------------------------------------
# 5) IAM for the function's invoker list:
#    - run.invoker → serviceAccount:$SA_EMAIL only.
#      Both Cloud Scheduler (production) AND test_fire.sh (manual) call the
#      function as $SA_EMAIL via OIDC token impersonation. The active gcloud
#      user is NOT added to the invoker list — it impersonates the SA below.
#    - iam.serviceAccountTokenCreator on $SA_EMAIL → granted to the active
#      gcloud user so test_fire.sh can mint an SA-signed OIDC token without
#      needing a downloaded JSON key.
# ---------------------------------------------------------------------------
header "--- Step 4: run.invoker + impersonation grants ---"
ACTIVE_USER=$(gcloud config get-value account 2>/dev/null || true)
if [[ -z "$ACTIVE_USER" ]]; then
  warn "No active gcloud account — skipping user-impersonation grant. Set one with 'gcloud auth login'."
fi

if [[ "$DRY_RUN" == true ]]; then
  info "[dry-run] would grant run.invoker on $FUNCTION_NAME to serviceAccount:$SA_EMAIL"
  [[ -n "$ACTIVE_USER" ]] && info "[dry-run] would grant iam.serviceAccountTokenCreator on $SA_EMAIL to user:$ACTIVE_USER"
else
  gcloud run services add-iam-policy-binding "$FUNCTION_NAME" \
    --region="$LOCATION" \
    --project="$PROJECT_ID" \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/run.invoker" >/dev/null
  ok "run.invoker → serviceAccount:$SA_EMAIL"

  if [[ -n "$ACTIVE_USER" ]]; then
    gcloud iam service-accounts add-iam-policy-binding "$SA_EMAIL" \
      --member="user:$ACTIVE_USER" \
      --role="roles/iam.serviceAccountTokenCreator" \
      --project="$PROJECT_ID" --quiet >/dev/null
    ok "iam.serviceAccountTokenCreator on $SA_EMAIL → user:$ACTIVE_USER  (lets test_fire.sh impersonate the SA)"
  fi
fi

# ---------------------------------------------------------------------------
# 6) Cloud Scheduler — bi-weekly, 21:00 ET.
# ---------------------------------------------------------------------------
header "--- Step 5: Cloud Scheduler job ---"
if [[ "$DRY_RUN" == true ]]; then
  info "[dry-run] would create/update job '$SCHEDULER_JOB' with cron='$SCHEDULE_CRON' tz='$SCHEDULE_TZ'"
else
  if gcloud scheduler jobs describe "$SCHEDULER_JOB" \
        --location="$LOCATION" --project="$PROJECT_ID" >/dev/null 2>&1; then
    info "Updating existing job '$SCHEDULER_JOB'..."
    gcloud scheduler jobs update http "$SCHEDULER_JOB" \
      --location="$LOCATION" \
      --project="$PROJECT_ID" \
      --schedule="$SCHEDULE_CRON" \
      --time-zone="$SCHEDULE_TZ" \
      --uri="$FN_URL" \
      --http-method=POST \
      --oidc-service-account-email="$SA_EMAIL" \
      --oidc-token-audience="$FN_URL"
  else
    info "Creating job '$SCHEDULER_JOB'..."
    gcloud scheduler jobs create http "$SCHEDULER_JOB" \
      --location="$LOCATION" \
      --project="$PROJECT_ID" \
      --schedule="$SCHEDULE_CRON" \
      --time-zone="$SCHEDULE_TZ" \
      --uri="$FN_URL" \
      --http-method=POST \
      --oidc-service-account-email="$SA_EMAIL" \
      --oidc-token-audience="$FN_URL"
  fi
  ok "Scheduler job ready: $SCHEDULER_JOB"
fi

echo
header "============================================"
ok "Dedup function deploy complete."
header "============================================"
info "Next firing : $SCHEDULE_CRON ($SCHEDULE_TZ)"
info ""
info "Trigger NOW (will back up + clean the master log if dups found):"
info "  bash cloud_function_dedup/test_fire.sh"
info ""
info "Trigger NOW in dry-run mode (scans + emails, no writes):"
info "  bash cloud_function_dedup/test_fire.sh --dry-run"
info ""
info "Tail logs:"
info "  gcloud functions logs read $FUNCTION_NAME --gen2 --region=$LOCATION --project=$PROJECT_ID --limit=50"
