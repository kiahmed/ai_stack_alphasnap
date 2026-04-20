#!/bin/bash
# run_local_test.sh
# Runs market_team.py (full MarketSweepApp across all enabled scouts) locally.
# Logs to dev-utils/run-logs/live_full_sweep_<timestamp>.log.

set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR/.."
source "$PROJECT_ROOT/ae_config.config"
export GOOGLE_APPLICATION_CREDENTIALS="$SCRIPT_DIR/service_account.json"

LOG_DIR="$SCRIPT_DIR/run-logs"
mkdir -p "$LOG_DIR"
TS=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$LOG_DIR/live_full_sweep_${TS}.log"

echo "Starting full-sweep local test at $(date)" | tee "$LOG_FILE"
echo "Log file: $LOG_FILE" | tee -a "$LOG_FILE"
echo "Storage/scouts config: $PROJECT_ROOT/values.yaml" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"

python3 "$PROJECT_ROOT/market_team.py" 2>&1 | tee -a "$LOG_FILE"

echo "" | tee -a "$LOG_FILE"
echo "Test finished at $(date)" | tee -a "$LOG_FILE"
echo "Log: $LOG_FILE"
