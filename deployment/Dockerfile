# ARMOR web + pipeline (GPU/GLiNER/Ollama typically run on host or sidecar).
# Build: docker build -f deployment/Dockerfile -t armor .
# Run:  docker run --rm -p 8765:8765 -v $(pwd)/db:/app/db -e OLLAMA_HOST=http://host.docker.internal:11434 armor

FROM python:3.11-slim-bookworm

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1
ENV FLASK_DEBUG=0

EXPOSE 8765

CMD ["gunicorn", "-c", "deployment/gunicorn.conf.py", "wsgi:app"]
