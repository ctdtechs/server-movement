VENV_PATH="/data/utility/mutility"
PROJECT_DIR="/data/workflow_v2/processing_status_report"
SCRIPT_PATH="$PROJECT_DIR/status_report.py"
LOG_FILE="/data/workflow_v2/cronlogs/psr_cron_job.log"
LOCK_FILE="/tmp/processing_status_report.lock"

if [ -f "$LOCK_FILE" ] && kill -0 "$(cat "$LOCK_FILE")" 2>/dev/null; then
    echo "[$(date)] Script is already running (PID: $(cat "$LOCK_FILE"))" >> "$LOG_FILE"
    exit 1
fi

echo $$ > "$LOCK_FILE"

trap 'rm -f "$LOCK_FILE"' EXIT

cd "$PROJECT_DIR" || { echo "[$(date)] cd to $PROJECT_DIR failed" >> "$LOG_FILE"; exit 1; }

source "$VENV_PATH/bin/activate"
python3.11 "$SCRIPT_PATH" --auto --grace 15 >> "$LOG_FILE" 2>&1
