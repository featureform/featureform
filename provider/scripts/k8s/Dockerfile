FROM python:3.10-slim

# Set environment variables for PostgreSQL
ENV POSTGRES_USER=myuser
ENV POSTGRES_PASSWORD=mypassword
ENV POSTGRES_DB=postgres

# Update the package list and install PostgreSQL client
RUN apt-get update && apt-get install -y postgresql-client gcc libpq-dev \
    && apt-get clean

WORKDIR /usr/app/src

COPY provider/scripts/k8s/offline_store_pandas_runner.py ./
COPY provider/scripts/k8s/requirements.txt ./

RUN pip install -r ./requirements.txt

CMD [ "python", "./offline_store_pandas_runner.py"]