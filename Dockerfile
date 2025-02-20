FROM node:18-alpine AS base

# Build the dashboard directory, only what is needed
FROM base AS deps
# Check https://github.com/nodejs/docker-node/tree/b4117f9333da4138b03a546ec926ef50a31506c3#nodealpine to understand why libc6-compat might be needed.
RUN apk add --no-cache libc6-compat
WORKDIR /app/dashboard

COPY /dashboard/package.json /dashboard/package-lock.json* ./
RUN npm i

# Rebuild the source code only when needed
FROM base AS builder
WORKDIR /app
COPY --from=deps /app/dashboard/node_modules ./dashboard/node_modules
COPY ./dashboard ./dashboard
ENV NEXT_TELEMETRY_DISABLED=1

WORKDIR /app/dashboard
RUN npm run build

# Production image, copy all the files and run next
FROM base AS runner
WORKDIR /app/dashboard
ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED=1
RUN addgroup --system --gid 1001 nodejs
RUN adduser --system --uid 1001 nextjs
COPY --from=builder /app/dashboard/public ./public

COPY --from=builder --chown=nextjs:nodejs /app/dashboard/.next/standalone ./
COPY --from=builder --chown=nextjs:nodejs /app/dashboard/.next/static ./.next/static
COPY --from=builder --chown=nextjs:nodejs /app/dashboard/out ./out

# Build Go services
FROM golang:1.22 AS go-builder

RUN apt update && \
    apt install -y protobuf-compiler
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

WORKDIR /app
COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY ./filestore/ ./filestore/
COPY ./health/ ./health/
COPY integrations/ integrations/
COPY fferr/ fferr/
COPY ffsync/ ffsync/
COPY scheduling/ scheduling/
COPY schema/ schema/
COPY storage/ storage/
COPY api/ api/
COPY lib/ lib/
COPY helpers/ helpers/
COPY metadata/ metadata/
COPY db/ db/
COPY metrics/ metrics/
COPY proto/ proto/
COPY coordinator/ coordinator/
COPY provider/ provider/
COPY runner/ runner/
COPY serving/ serving/
COPY types/ types/
COPY kubernetes/ kubernetes/
COPY config/ config/
COPY logging/ logging/
COPY ./streamer_proxy/ streamer_proxy/

RUN protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative ./proto/serving.proto
RUN protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative ./metadata/proto/metadata.proto
RUN protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative ./scheduling/proto/scheduling.proto

RUN mkdir execs
RUN go build -o execs/api api/main/main.go
RUN go build -o execs/metadata metadata/server/server.go
RUN go build -o execs/coordinator coordinator/main/main.go
RUN go build -o execs/dashboard_metadata metadata/dashboard/main/main.go
RUN go build -o execs/serving serving/main/main.go
RUN go build -o execs/streamer_proxy streamer_proxy/main.go

# Build Python Streamer
FROM python:3.10 AS streamer-builder

WORKDIR /app/streamer

RUN apt-get update && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --break-system-packages --upgrade pip
RUN pip install --break-system-packages boto3 pyarrow 'pyiceberg[glue]'

COPY ./streamer/ /app/streamer/

# Final Image
FROM golang:1.22

WORKDIR /app

# Install Python for the streamer to work
RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-pip build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --break-system-packages --upgrade pip
RUN pip install --break-system-packages boto3 pyarrow 'pyiceberg[glue]'

# Copy the Python virtual environment
COPY --from=streamer-builder /app/streamer /app/streamer

ENV PATH="/app/venv/bin:$PATH"

# Install and configure Supervisor
RUN apt-get update && apt-get install -y supervisor
RUN mkdir -p /var/lock/apache2 /var/run/apache2 /var/run/sshd /var/log/supervisor
RUN apt-get install -y nginx --option=Dpkg::Options::=--force-confdef

# Install Node 18 for internal dashboard server
RUN curl -sL https://deb.nodesource.com/setup_18.x | sh
RUN apt-get update
RUN apt-get install -y nodejs

# Install MeiliSearch
# RUN curl -L https://install.meilisearch.com | sh

# Install goose for migrations
RUN go install github.com/pressly/goose/v3/cmd/goose@v3.18.0

# Copy migrations directory
COPY db/migrations /app/db/migrations

# Install and initialize internal postgres for app state
RUN apt-get update && apt-get install -y postgresql postgresql-contrib
USER postgres
RUN /etc/init.d/postgresql start && \
    psql --command "ALTER USER postgres WITH PASSWORD 'password';" && \
    goose -dir /app/db/migrations postgres "host=localhost user=postgres password=password dbname=postgres sslmode=disable" up
USER root

COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Copy built dashboard
COPY --from=runner /app/dashboard ./dashboard

# Copy Python Streamer
COPY --from=streamer-builder /app/streamer /app/streamer

# Setup Spark
ARG SPARK_FILEPATH=/app/provider/scripts/spark/offline_store_spark_runner.py
ARG SPARK_PYTHON_PACKAGES=/app/provider/scripts/spark/python_packages.sh
ARG SPARK_REQUIREMENTS=/app/provider/scripts/spark/requirements.txt
ARG MATERIALIZE_NO_TIMESTAMP_QUERY_PATH=/app/provider/queries/materialize_no_ts.sql
ARG MATERIALIZE_TIMESTAMP_QUERY_PATH=/app/provider/queries/materialize_ts.sql

COPY provider/scripts/spark/offline_store_spark_runner.py $SPARK_FILEPATH
COPY provider/scripts/spark/python_packages.sh $SPARK_PYTHON_PACKAGES
COPY provider/scripts/spark/requirements.txt $SPARK_REQUIREMENTS
COPY provider/queries/materialize_no_ts.sql $MATERIALIZE_NO_TIMESTAMP_QUERY_PATH
COPY provider/queries/materialize_ts.sql $MATERIALIZE_TIMESTAMP_QUERY_PATH

# Take the MD5 hash of the Spark runner script and store it in a file for use by the config package
# when determining the remove filepath in cloud object storage (e.g. S3). By adding the hash as a suffix
# to the file, we ensure that different versions of the script are uploaded to cloud object storage
# without overwriting previous or future versions.
RUN cat /app/provider/scripts/spark/offline_store_spark_runner.py | md5sum \
    | awk '{print $1}' \
    | xargs echo -n > /app/provider/scripts/spark/offline_store_spark_runner_md5.txt

ENV PYTHON_LOCAL_INIT_PATH=$SPARK_PYTHON_PACKAGES

# Setup Nginx
COPY nginx.conf /etc/nginx/nginx.conf

# Copy built Go services
COPY --from=go-builder /app/execs /app/execs

# Copy built dashboard
COPY --from=runner /app/dashboard ./dashboard

ENV SERVING_PORT="8082"
ENV SERVING_HOST="0.0.0.0"
ENV FEATUREFORM_HOST="localhost"
ENV FF_STATE_PROVIDER="psql"
ENV USE_CLIENT_MODE="true"

EXPOSE 7878
EXPOSE 80
EXPOSE 5432
EXPOSE 8085
EXPOSE 8086

COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

HEALTHCHECK --interval=5m --timeout=10s --start-period=10s --retries=3 CMD curl --fail http://localhost/ || exit 1

CMD ["/usr/bin/supervisord"]
