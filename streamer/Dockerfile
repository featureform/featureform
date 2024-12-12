FROM python:3.10-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --upgrade pip
RUN pip install boto3 pyarrow 'pyiceberg[glue]'

COPY ./streamer/ /app

EXPOSE 8085

CMD ["python", "iceberg_streamer.py"]