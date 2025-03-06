FROM postgres:15-alpine

ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=password
ENV POSTGRES_DB=postgres

# Copy both the init script and CSV into the Docker init directory
COPY init.sql /docker-entrypoint-initdb.d/
COPY transactions.csv /docker-entrypoint-initdb.d/
