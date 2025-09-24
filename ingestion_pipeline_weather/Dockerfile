FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ingestion_openmeteo.py .
CMD ["python", "-u", "ingestion_openmeteo.py"]
