# Hosting & deployment

## Gunicorn (recommended)

From the **repository root** (with venv activated):

```bash
gunicorn -c deployment/gunicorn.conf.py wsgi:app
```

Override bind/workers via env:

- `GUNICORN_BIND` (default `0.0.0.0:8765`)
- `GUNICORN_WORKERS`, `GUNICORN_THREADS`, `GUNICORN_TIMEOUT` (default `3600` for long pipeline runs)

Set **`ARMOR_SECRET_KEY`** (and optionally **`FLASK_DEBUG=0`**) in production.

## Docker

```bash
docker build -f deployment/Dockerfile -t armor .
docker run --rm -p 8765:8765 \
  -v "$(pwd)/db:/app/db" \
  -v "$(pwd)/instance:/app/instance" \
  -e ARMOR_SECRET_KEY=... \
  -e OLLAMA_HOST=http://host.docker.internal:11434 \
  armor
```

Heavy ML (GLiNER, Ollama) is often run **on the host** or a **GPU sidecar**; this image focuses on the Flask app and subprocess pipeline.

## Reverse proxy

Terminate TLS in **nginx** or **AWS ALB** and proxy to `http://127.0.0.1:8765`. Set `X-Forwarded-*` headers if you need correct external URLs.

## Systemd (sketch)

```ini
[Service]
WorkingDirectory=/opt/ner-anonimyser
Environment=ARMOR_SECRET_KEY=...
ExecStart=/opt/ner-anonimyser/.venv/bin/gunicorn -c deployment/gunicorn.conf.py wsgi:app
```
