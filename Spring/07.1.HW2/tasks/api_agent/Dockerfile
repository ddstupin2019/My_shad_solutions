FROM python:3.13-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8000 \
    DB_PATH=/app/data/app.db

WORKDIR /app

RUN addgroup --system app && adduser --system --ingroup app app

COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY pyproject.toml README.md ./
COPY api_agent ./api_agent

RUN pip install --no-cache-dir --no-deps .

RUN mkdir -p /app/data && chown -R app:app /app

USER app

EXPOSE 8000

CMD ["sh", "-c", "uvicorn api_agent.app:app --host 0.0.0.0 --port ${PORT}"]
