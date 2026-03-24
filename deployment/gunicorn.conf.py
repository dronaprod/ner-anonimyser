# Gunicorn — production WSGI server (run from repository root).
#
#   cd /path/to/ner-anonimyser && gunicorn -c deployment/gunicorn.conf.py wsgi:app
#
import multiprocessing
import os

bind = os.environ.get("GUNICORN_BIND", "0.0.0.0:8765")
workers = int(os.environ.get("GUNICORN_WORKERS", max(2, multiprocessing.cpu_count() // 2)))
threads = int(os.environ.get("GUNICORN_THREADS", "4"))
timeout = int(os.environ.get("GUNICORN_TIMEOUT", "3600"))
graceful_timeout = 120
worker_class = "gthread"
accesslog = "-"
errorlog = "-"
loglevel = os.environ.get("GUNICORN_LOG_LEVEL", "info")
capture_output = True
# Pipeline subprocess can run a long time; keep worker alive
max_requests = int(os.environ.get("GUNICORN_MAX_REQUESTS", "500"))
max_requests_jitter = 50
