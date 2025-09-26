# Slim Python + system deps
FROM python:3.11-slim

# System packages (git for dbt deps if you add packages.yml)
RUN apt-get update && apt-get install -y --no-install-recommends \
    git ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Python deps
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# dbt project
COPY dbt_project.yml /app/dbt_project.yml
COPY profiles /app/profiles
COPY macros /app/macros
COPY models /app/models

# Default env placeholders (Cloud Run Job will override)
ENV PROJECT=""
ENV GOOGLE_CLOUD_PROJECT=""
ENV REGION="europe-north2"
ENV DBT_SILVER="silver"
ENV DBT_GOLD="gold"

# Simple runner
CMD bash -lc "\
  echo '== dbt debug =='; \
  dbt debug --profiles-dir /app/profiles --project-dir /app || exit 2; \
  echo '== dbt run =='; \
  dbt run   --profiles-dir /app/profiles --project-dir /app --target prod \
"
