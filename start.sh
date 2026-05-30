#!/bin/bash
# Wrapper that runs the grab pipeline while serving the live dashboard on port 8080.
# The pipeline command (and any extra args) is passed in via "$@".
# stdout+stderr of the pipeline is tee'd to both docker-logs and the dashboard's log file.
set -euo pipefail

LOGFILE=/tmp/grab.log
DASH_PORT=${DASH_PORT:-8080}

touch "$LOGFILE"

python3 /grab/dashboard.py "$LOGFILE" --no-browser --port "$DASH_PORT" &
DASH_PID=$!

cleanup() {
    kill "$DASH_PID" 2>/dev/null || true
    rm -f "$LOGFILE"
}
trap cleanup EXIT INT TERM

# Run the pipeline (base image CMD arrives as positional args), tee output so
# both "docker logs" and the dashboard receive every line.
"$@" 2>&1 | tee -a "$LOGFILE"
