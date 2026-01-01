#!/bin/bash
# Run insider trading pipeline
# This script is designed to be run periodically by cron.

set -euo pipefail

# Configuration
PROJECT_DIR="/home/williamewanchuk/Personal/RandomScripts/queries/insidertrading"
WRAPPER_LOG="${PROJECT_DIR}/logging/cron_wrapper.log"
LOCK_FILE="${PROJECT_DIR}/logging/pipeline.lock"

# Logging function
log() {
    local level="$1"
    shift
    local message="$*"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$level] $message" | tee -a "$WRAPPER_LOG"
}

# Change to project directory
cd "$PROJECT_DIR" || {
    log "ERROR" "Failed to change to project directory: $PROJECT_DIR"
    exit 1
}

# Check for existing lock
if [ -f "$LOCK_FILE" ]; then
    LOCK_PID=$(cat "$LOCK_FILE")
    if ps -p "$LOCK_PID" > /dev/null 2>&1; then
        log "ERROR" "Another instance is running (PID: $LOCK_PID)"
        exit 1
    else
        log "WARN" "Stale lock file found (PID: $LOCK_PID). Removing."
        rm -f "$LOCK_FILE"
    fi
fi

# Acquire lock
echo $$ > "$LOCK_FILE"
log "INFO" "Lock acquired (PID: $$)"

# Start postgres
log "INFO" "Starting postgres service..."
docker compose up -d postgres

# Wait for postgres to be ready
log "INFO" "Waiting for postgres to be ready..."
sleep 5

# Run the pipeline
log "INFO" "Executing pipeline container..."
if docker compose run --rm trackingcontainer; then
    log "INFO" "Pipeline completed successfully"
    EXIT_CODE=0
else
    EXIT_CODE=$?
    log "ERROR" "Pipeline failed with exit code: $EXIT_CODE"
fi

# Stop all services
log "INFO" "Stopping services..."
docker compose down

# Release lock
rm -f "$LOCK_FILE"
log "INFO" "Lock released"
log "INFO" "Pipeline finished"

exit $EXIT_CODE
