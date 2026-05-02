#!/usr/bin/env bash
# ============================================================================
# cloud_function_dedup/test_fire.sh — manually trigger the dedup function.
#
# Usage:
#   bash cloud_function_dedup/test_fire.sh              # PROD: writes + emails
#   bash cloud_function_dedup/test_fire.sh --dry-run    # scans + emails, no writes
#   bash cloud_function_dedup/test_fire.sh --yes        # skip confirmation
#
# Side-effects (non-dry-run):
#   - If duplicates are found:
#       1) backs up the master log to gs://<bucket>/backups/.
#       2) writes a cleaned master log replacing the originals (newer dup
#          dropped, older kept).
#       3) emails arboryx.platform@gmail.com with the report.
#   - If no duplicates: ONLY a structured log line. NO email. NO writes.
#
# Invocation modes:
#   - default → Cloud Scheduler `jobs run` (mirrors the bi-weekly path)
#   - if you pass --dry-run we skip Cloud Scheduler and POST directly with
#     ?dry_run=true so the path is exercised as the function would receive it.
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck disable=SC1090
source "$REPO_ROOT/ae_config.config"
# shellcheck disable=SC1090
source "$SCRIPT_DIR/dedup_function.config"

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
YES=false
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    --yes|-y)  YES=true ;;
    *) err "unknown arg: $arg"; exit 2 ;;
  esac
done

header "============================================"
header "  Arboryx Dedup — Test Fire"
header "============================================"
info "Function   : $FUNCTION_NAME"
info "Scheduler  : $SCHEDULER_JOB"
info "Bucket     : gs://$GCS_BUCKET/$GCS_OBJECT"
info "Project    : $PROJECT_ID"
info "Region     : $LOCATION"
info "Mode       : $([[ $DRY_RUN == true ]] && echo 'DRY-RUN (no writes)' || echo 'PROD (will mutate master log)')"
echo

if [[ "$YES" != true ]]; then
  if [[ "$DRY_RUN" == true ]]; then
    warn "Dry-run: function will scan + email but not modify the master log."
  else
    warn "PROD: if duplicates are found the master log WILL be backed up + rewritten."
    warn "      A backup copy lands in gs://$GCS_BUCKET/${BACKUP_PREFIX}master_findings_log.backup-<UTC>.json"
  fi
  read -r -p "Continue? (y/N) " confirm
  [[ "$confirm" == "y" || "$confirm" == "Y" ]] || { info "Aborted."; exit 0; }
  echo
fi

if [[ "$DRY_RUN" == true ]]; then
  # POST directly with ?dry_run=true. Need an OIDC token from the SA.
  header "--- Direct POST (dry-run) ---"
  FN_URL=$(gcloud functions describe "$FUNCTION_NAME" \
    --gen2 --region="$LOCATION" --project="$PROJECT_ID" \
    --format='value(serviceConfig.uri)' 2>/dev/null || true)
  if [[ -z "$FN_URL" ]]; then
    err "Could not resolve function URL — is it deployed?"; exit 1
  fi
  # Mint the OIDC token AS $SA_EMAIL by impersonation. The function only
  # whitelists $SA_EMAIL on its invoker list — same identity Cloud Scheduler
  # uses for the prod path. The active gcloud user just needs
  # iam.serviceAccountTokenCreator on $SA_EMAIL (granted by deploy.sh).
  TOKEN=$(gcloud auth print-identity-token \
    --impersonate-service-account="$SA_EMAIL" \
    --audiences="$FN_URL")
  # -w prints the HTTP status so a 403/404 is visible even if the body is empty.
  # No `|| true` — surface failures loudly.
  HTTP_CODE=$(curl -sS -X POST "$FN_URL?dry_run=true" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    --data '{}' \
    -w '\nHTTP_STATUS=%{http_code}\n' | tee /dev/tty | awk -F= '/^HTTP_STATUS=/{print $2}')
  echo
  if [[ "$HTTP_CODE" != "200" ]]; then
    err "Function returned HTTP $HTTP_CODE — function was NOT invoked successfully."
    err "  Common causes:"
    err "    - $(gcloud config get-value account 2>/dev/null) lacks iam.serviceAccountTokenCreator on $SA_EMAIL"
    err "      (deploy.sh grants this automatically — re-run deploy.sh if you skipped it)"
    err "    - $SA_EMAIL lacks run.invoker on the function"
    err "    - Function returned 5xx before logging — check Cloud Run logs"
    exit 1
  fi
else
  header "--- Firing scheduler job ---"
  gcloud scheduler jobs run "$SCHEDULER_JOB" \
    --location="$LOCATION" --project="$PROJECT_ID"
  ok "Scheduler kicked. Cloud Scheduler will POST to the function within ~5s."
fi

echo
info "Waiting 15s for invocation to complete..."
sleep 15

header "--- Last 60 function log lines ---"
gcloud functions logs read "$FUNCTION_NAME" \
  --gen2 --region="$LOCATION" --project="$PROJECT_ID" --limit=60 2>&1 | head -100
echo

header "============================================"
ok "Test fire dispatched."
header "============================================"
info "Look for 'dedup_complete {...}' with status=ok above."
info "If duplicates were found in PROD mode, an email is in $GMAIL_TO."
info "Backups list:"
info "  gcloud storage ls gs://$GCS_BUCKET/$BACKUP_PREFIX"
