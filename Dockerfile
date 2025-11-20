FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY load_kafka.py .

CMD ["python", "-u", "load_kafka.py"]

