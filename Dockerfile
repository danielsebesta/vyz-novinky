FROM python:3.12-slim

ENV TZ=Europe/Prague

# Install cron + timezone data
RUN apt-get update && \
    apt-get install -y --no-install-recommends cron tzdata && \
    ln -sf /usr/share/zoneinfo/$TZ /etc/localtime && \
    echo $TZ > /etc/timezone && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY main.py .
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

# Output directory for debug logs, JSON, and lockfiles
# Must be a volume so lockfiles survive container restarts
RUN mkdir -p /app/daily_questions
VOLUME /app/daily_questions

CMD ["./entrypoint.sh"]
