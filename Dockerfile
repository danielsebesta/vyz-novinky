FROM python:3.12-slim

ENV TZ=Europe/Prague

# Install timezone data and curl for downloading wireproxy
RUN apt-get update && \
    apt-get install -y --no-install-recommends tzdata curl && \
    ln -sf /usr/share/zoneinfo/$TZ /etc/localtime && \
    echo $TZ > /etc/timezone && \
    rm -rf /var/lib/apt/lists/*

# Install wireproxy for Cloudflare WARP SOCKS5 fallback
RUN ARCH=$(dpkg --print-architecture) && \
    curl -fsSL "https://github.com/pufferffish/wireproxy/releases/download/v1.0.9/wireproxy_linux_${ARCH}.tar.gz" \
    | tar -xz -C /usr/local/bin/ wireproxy && \
    chmod +x /usr/local/bin/wireproxy

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY main.py .
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

# Output directory for debug logs and JSON
RUN mkdir -p /app/daily_questions
VOLUME /app/daily_questions

CMD ["./entrypoint.sh"]
