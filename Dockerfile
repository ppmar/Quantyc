FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Data directory for SQLite DB (PDFs are deleted after parsing)
RUN mkdir -p data/raw

ENV PORT=8000
EXPOSE ${PORT}

CMD gunicorn app:app --bind 0.0.0.0:${PORT} --workers 2 --timeout 120
