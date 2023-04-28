FROM node:16-alpine AS base

# Build the dashboard directoy, only what is needed.
# Install dependencies only when needed
FROM base AS deps
# Check https://github.com/nodejs/docker-node/tree/b4117f9333da4138b03a546ec926ef50a31506c3#nodealpine to understand why libc6-compat might be needed.
RUN apk add --no-cache libc6-compat
WORKDIR /app/dashboard

# Install dependencies based on the preferred package manager
COPY /dashboard/package.json /dashboard/.yarn.lock* /dashboard/package-lock.json* ./
RUN npm install --legacy-peer-deps

# Rebuild the source code only when needed
FROM base AS builder
WORKDIR /app
COPY --from=deps /app/dashboard/node_modules ./dashboard/node_modules
COPY ./dashboard ./dashboard
ENV NEXT_TELEMETRY_DISABLED 1

WORKDIR /app/dashboard
RUN npm run build

#Production image, copy all the files and run next
FROM base AS runner
WORKDIR /app/dashboard
ENV NODE_ENV production
ENV NEXT_TELEMETRY_DISABLED 1
RUN addgroup --system --gid 1001 nodejs
RUN adduser --system --uid 1001 nextjs
COPY --from=builder /app/dashboard/public ./public

COPY --from=builder --chown=nextjs:nodejs /app/dashboard/.next/standalone ./
COPY --from=builder --chown=nextjs:nodejs /app/dashboard/.next/static ./.next/static
COPY --from=builder --chown=nextjs:nodejs /app/dashboard/out ./out

# Build Go services
FROM golang:1.18 as go-builder

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
COPY api/ api/
COPY helpers/ helpers/
COPY metadata/ metadata/
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

RUN apt update && \
    apt install -y protobuf-compiler
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
RUN protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative ./proto/serving.proto
RUN protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative ./metadata/proto/metadata.proto

RUN mkdir execs
RUN go build -o execs/api api/main.go
RUN go build -o execs/metadata metadata/server/server.go
RUN go build -o execs/coordinator coordinator/main/main.go
RUN go build -o execs/dashboard_metadata metadata/dashboard/dashboard_metadata.go
RUN go build -o execs/serving serving/main/main.go

# Final image
FROM golang:1.18

WORKDIR /app

# Copy built Go services
COPY --from=go-builder /app/execs /app/execs

# Copy built dashboard
COPY --from=runner /app/dashboard ./dashboard

# Install and configure Supervisor
RUN apt-get update && apt-get install -y supervisor
RUN mkdir -p /var/lock/apache2 /var/run/apache2 /var/run/sshd /var/log/supervisor
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Setup Spark
COPY provider/scripts/spark/offline_store_spark_runner.py scripts/spark/offline_store_spark_runner.py
COPY provider/scripts/spark/python_packages.sh scripts/spark/python_packages.sh
COPY provider/scripts/spark/requirements.txt scripts/spark/requirements.txt

# Setup Etcd
RUN git clone -b v3.4.16 https://github.com/etcd-io/etcd.git
WORKDIR /app/etcd
RUN ./build
WORKDIR /app
RUN ETCD_UNSUPPORTED_ARCH=arm64 ./etcd/bin/etcd --version

# Install Nginx
RUN apt-get update
RUN apt-get install -y nginx --option=Dpkg::Options::=--force-confdef
COPY nginx.conf /etc/nginx/nginx.conf

# Install MeiliSearch
RUN curl -L https://install.meilisearch.com | sh

# Install Node 16 for internal dashboard server
RUN curl -sL https://deb.nodesource.com/setup_16.x | sh
RUN apt-get update
RUN apt install nodejs

ENV SERVING_PORT="8082"
ENV SERVING_HOST="0.0.0.0"
ENV ETCD_ARCH=""

EXPOSE 7878
EXPOSE 3005

CMD ["/usr/bin/supervisord"]
