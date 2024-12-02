FROM python:3.10-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY ./streamer/requirements.txt /app/
COPY ./streamer/streamer.py /app/
COPY ./streamer/table_data_short.csv /app/
COPY ./streamer/table_data_long.csv /app/

RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

# todo: change this later
EXPOSE 8085

CMD ["python", "streamer.py"]