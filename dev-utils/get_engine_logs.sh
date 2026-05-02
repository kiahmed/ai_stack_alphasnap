#!/usr/bin/env bash
# Fetch ReasoningEngine logs from Cloud Logging for the engine in ae_config.config.
# Defaults to today (00:00:00Z) unless --date / --time are supplied.
# Output is always written to dev-utils/output.log next to this script.
#
# Usage:
#   ./get_engine_logs.sh                              # today, from 00:00:00Z
#   ./get_engine_logs.sh --date 2026-04-26            # specific date, 00:00:00Z
#   ./get_engine_logs.sh --date 2026-04-26 --time 13:30:00
#   ./get_engine_logs.sh --time 09:00:00              # today at 09:00:00Z
#   ./get_engine_logs.sh --limit 1000                 # override gcloud --limit

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CONFIG_FILE="$REPO_ROOT/ae_config.config"
OUTPUT_FILE="$SCRIPT_DIR/output.log"

if [ ! -f "$CONFIG_FILE" ]; then
    echo "❌ ae_config.config not found at $CONFIG_FILE" >&2
    exit 1
fi

# shellcheck disable=SC1090
source "$CONFIG_FILE"

: "${ENGINE_ID:?ENGINE_ID not set in ae_config.config}"
: "${PROJECT_ID:?PROJECT_ID not set in ae_config.config}"
: "${LOCATION:?LOCATION not set in ae_config.config}"

DATE_ARG=""
TIME_ARG=""
LIMIT=5000

while [ $# -gt 0 ]; do
    case "$1" in
        --date)
            DATE_ARG="$2"; shift 2 ;;
        --time)
            TIME_ARG="$2"; shift 2 ;;
        --limit)
            LIMIT="$2"; shift 2 ;;
        -h|--help)
            sed -n '2,12p' "$0"; exit 0 ;;
        *)
            echo "Unknown arg: $1" >&2; exit 2 ;;
    esac
done

DATE_PART="${DATE_ARG:-$(date -u +%Y-%m-%d)}"
TIME_PART="${TIME_ARG:-00:00:00}"
TIMESTAMP="${DATE_PART}T${TIME_PART}Z"

FILTER="resource.type=\"aiplatform.googleapis.com/ReasoningEngine\" \
AND resource.labels.location=\"$LOCATION\" \
AND resource.labels.reasoning_engine_id=\"$ENGINE_ID\" \
AND timestamp>=\"$TIMESTAMP\""

echo "📡 engine=$ENGINE_ID  project=$PROJECT_ID  since=$TIMESTAMP  limit=$LIMIT"
echo "📝 writing → $OUTPUT_FILE"

gcloud logging read "$FILTER" \
    --limit="$LIMIT" \
    --project="$PROJECT_ID" \
    --format="value(textPayload)" \
    > "$OUTPUT_FILE"

LINES=$(wc -l < "$OUTPUT_FILE" | tr -d ' ')
echo "✅ wrote $LINES lines"
