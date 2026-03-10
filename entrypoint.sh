#!/bin/bash
set -e

# Dump all environment variables into a file that cron can source
# (cron runs in a clean shell with no env vars by default)
printenv | grep -v "no_proxy" > /etc/environment

# Set up the cron job — every day at 12:00 Prague time
echo "0 12 * * * root cd /app && /usr/local/bin/python main.py >> /var/log/quiz.log 2>&1 && touch /app/daily_questions/.last_run_\$(date +\%Y-\%m-\%d)" > /etc/cron.d/quiz-cron
chmod 0644 /etc/cron.d/quiz-cron
crontab /etc/cron.d/quiz-cron

# Create log file
touch /var/log/quiz.log

echo "[$(date)] Container started. Cron scheduled for 12:00 Prague time daily."

# Run once on startup — but only if it hasn't run today yet
# Uses a date-stamped lockfile so it resets daily
TODAY=$(date +%Y-%m-%d)
LOCKFILE="/app/daily_questions/.last_run_${TODAY}"

if [ ! -f "$LOCKFILE" ]; then
    echo "[$(date)] No run today yet, executing now..."
    cd /app && python main.py 2>&1 | tee -a /var/log/quiz.log
    touch "$LOCKFILE"
    # Clean up old lockfiles
    find /app/daily_questions -name ".last_run_*" ! -name ".last_run_${TODAY}" -delete 2>/dev/null || true
    echo "[$(date)] Done. Next run at 12:00 tomorrow."
else
    echo "[$(date)] Already ran today (lockfile: $LOCKFILE). Waiting for cron."
fi

# Keep container alive by tailing the log + running cron in foreground
cron && tail -f /var/log/quiz.log
