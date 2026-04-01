#!/bin/bash
# run_local_test.sh
# Runs market_team.py locally, tees all output (stdout + stderr) to output.log

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR/.."
source "$PROJECT_ROOT/ae_config.config"
export GOOGLE_APPLICATION_CREDENTIALS="$SCRIPT_DIR/service_account.json"

LOG_FILE="output.log"

echo "Starting local sweep test at $(date)" | tee "$LOG_FILE"
echo "Log file: $LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"

python3 "$PROJECT_ROOT/market_team.py" 2>&1 | tee -a "$LOG_FILE"

echo "" | tee -a "$LOG_FILE"
echo "Test finished at $(date)" | tee -a "$LOG_FILE"
