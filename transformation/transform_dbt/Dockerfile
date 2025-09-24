# Python slim is fine
FROM python:3.11-slim

# System deps (git for dbt debug/packages; gcc for some wheels)
RUN apt-get update -y \
  && apt-get install -y --no-install-recommends git build-essential \
  && rm -rf /var/lib/apt/lists/*

# Workdir
WORKDIR /app

# Python deps
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Project files
COPY dbt_project.yml /app/dbt_project.yml
COPY models /app/models
COPY profiles /app/profiles
# (Optional) if you later add seeds/
# COPY seeds /app/seeds

# Make sure dbt is on PATH and show version in build logs (nice cache layer)
RUN dbt --version

# Default env for dbt to find profiles
ENV DBT_PROFILES_DIR=/app/profiles

# Run a quick smoketest + the transformations
CMD sh -c "dbt debug && dbt seed --full-refresh || true && dbt run"
