FROM mirror.gcr.io/library/python:3.11-slim
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY ingestion_openaq.py ./
ENV PYTHONUNBUFFERED=1
CMD ["python", "ingestion_openaq.py"]
