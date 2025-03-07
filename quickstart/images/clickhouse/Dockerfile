FROM clickhouse/clickhouse-server:latest

RUN mkdir -p /docker-entrypoint-initdb.d
COPY transactions.csv /docker-entrypoint-initdb.d/transactions.csv
COPY init.sql /docker-entrypoint-initdb.d/init.sql

RUN chmod -R 755 /docker-entrypoint-initdb.d