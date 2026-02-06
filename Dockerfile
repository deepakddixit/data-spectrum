# Build Stage
FROM python:3.10-slim as builder

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_VERSION=1.7.1 \
    POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_NO_INTERACTION=1

# Install Poetry
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl build-essential \
    && curl -sSL https://install.python-poetry.org | python3 -

ENV PATH="$POETRY_HOME/bin:$PATH"

COPY pyproject.toml poetry.lock ./
RUN poetry install --no-root --only main

# Runtime Stage
FROM python:3.10-slim as runtime

WORKDIR /app

ENV VIRTUAL_ENV=/app/.venv \
    PATH="/app/.venv/bin:$PATH" \
    METADATA_DB_PATH="/data/metadata.db" \
    METADATA_TTL=86400 \
    DISCOVERY_TTL=3600

# Install runtime dependencies for DuckDB extensions (curl, ca-certificates)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create persistence directory
RUN mkdir -p /data && chmod 777 /data
VOLUME /data

COPY --from=builder /app/.venv /app/.venv
COPY src ./src
COPY src ./src
COPY utils ./utils
COPY README.md .
COPY bin/cli /usr/local/bin/cli
RUN chmod +x /usr/local/bin/cli

# Pre-install DuckDB extensions to bake them into the image
# RUN /app/.venv/bin/python -c "import duckdb; con = duckdb.connect(); con.install_extension('tpcds'); con.install_extension('httpfs');"

ENV PYTHONPATH=/app

EXPOSE 8000

CMD ["uvicorn", "src.api.server:app", "--host", "0.0.0.0", "--port", "8000"]
