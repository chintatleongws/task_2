FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    POETRY_VIRTUALENVS_CREATE=false

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip poetry

COPY pyproject.toml poetry.lock* README.md ./
COPY . .

RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi --no-root

CMD ["python", "worker.py"]
