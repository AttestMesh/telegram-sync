FROM python:3.12-slim

WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# Session volume (Telethon session file lives here); run unprivileged
RUN useradd -r -m -d /home/tgsync tgsync && mkdir -p /data && chown tgsync:tgsync /data
VOLUME /data
USER tgsync

ENV SESSION_DIR=/data

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s \
    CMD python -c "import urllib.request,sys,os; sys.exit(0 if urllib.request.urlopen(f'http://localhost:{os.environ.get(\"HEALTH_PORT\",\"8082\")}/health', timeout=4).status==200 else 1)"

CMD ["python", "-m", "app.main"]
