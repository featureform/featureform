FROM python:3.10-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY ./streamer/requirements.txt /app/
COPY ./streamer/iceberg_streamer.py /app/

RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

EXPOSE 8085

CMD ["python", "iceberg_streamer.py"]