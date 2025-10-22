#!/bin/bash
# Wrapper script to run OpenAQ pipeline safely and automatically

# Config block - change PROJECT_DIR to your absolute dir
PROJECT_DIR="/absolute/path/to/your/project"
VENV_PATH="$PROJECT_DIR/.venv/bin/activate"
LOG_DIR="$PROJECT_DIR/data/raw"
LOCK_FILE="$PROJECT_DIR/.job.lock"

# Create log directory if missing
mkdir -p "$LOG_DIR"

# Get current UTC date for log naming and prep for log creation
DATE=$(date -u +'%y-%m-%d')
LOG_FILE="$LOG_DIR/fetch_$DATE.log"

# Prevent overlapping runs
if [ -f "$LOCK_FILE" ]; then
    echo "$(date -u): Lock file exists ($LOCK_FILE) — another job is likely running. Exiting." >> "$LOG_FILE"
    exit 1
else
    # Attempt to create the lock file
    touch "$LOCK_FILE"
    if [ -f "$LOCK_FILE" ]; then
        echo "$(date -u): Lock file created successfully at $LOCK_FILE" >> "$LOG_FILE"
    else
        echo "$(date -u): Failed to create lock file at $LOCK_FILE — exiting." >> "$LOG_FILE"
        exit 1
    fi
fi

#Off we go.
echo "$(date -u): Starting OpenAQ pipeline..." >> "$LOG_FILE"
source "$VENV_PATH"
python "$PROJECT_DIR/main.py" >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

#cleaning
rm -f "$LOCK_FILE"

if [ $EXIT_CODE -ne 0 ]; then
    echo "$(date -u): Pipeline failed with exit code $EXIT_CODE" >> "$LOG_FILE"
else
    echo "$(date -u): Pipeline completed successfully." >> "$LOG_FILE"
fi
