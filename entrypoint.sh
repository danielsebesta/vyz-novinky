#!/bin/bash
set -e

# Dump all environment variables into a file that cron can source
# (cron runs in a clean shell with no env vars by default)
printenv | grep -v "no_proxy" > /etc/environment

# Set up the cron job — every day at 12:00 UTC
echo "0 12 * * * root cd /app && /usr/local/bin/python main.py >> /var/log/quiz.log 2>&1" > /etc/cron.d/quiz-cron
chmod 0644 /etc/cron.d/quiz-cron
crontab /etc/cron.d/quiz-cron

# Create log file
touch /var/log/quiz.log

echo "[$(date)] Container started. Cron scheduled for 12:00 Prague time daily."
echo "[$(date)] Next run: $(date -d 'tomorrow 12:00' 2>/dev/null || echo 'check with: crontab -l')"

# Optional: run immediately on first deploy (remove if you don't want this)
if [ "${RUN_ON_START:-false}" = "true" ]; then
    echo "[$(date)] RUN_ON_START=true, executing now..."
    cd /app && python main.py 2>&1 | tee -a /var/log/quiz.log
fi

# Keep container alive by tailing the log + running cron in foreground
cron && tail -f /var/log/quiz.log
