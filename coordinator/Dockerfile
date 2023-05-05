
# Build stage
FROM golang:1.18-alpine AS builder

WORKDIR /app

# Copy go mod and sum files, then download dependencies using a cache
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY ./metadata/proto/metadata.proto ./metadata/proto/metadata.proto
RUN apk update && \
    apk add protobuf-dev && \
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest && \
    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest && \
    export PATH=$PATH:/go/bin && \
    protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative ./metadata/proto/metadata.proto

COPY ./coordinator/*.go ./coordinator/
COPY ./provider/ ./provider/
COPY ./config/ ./config/
COPY ./helpers/ ./helpers/
COPY ./logging/ ./logging/
COPY ./metadata/ ./metadata/
COPY ./runner/ ./runner/
COPY ./kubernetes ./kubernetes
COPY ./types ./types
COPY ./coordinator/main/main.go ./coordinator/main/main.go

ENV CGO_ENABLED=0
RUN go build -o /app/main ./coordinator/main/main.go

# Final stage
FROM alpine:3.15

COPY --from=builder /app/main /app/main

# Download Shaded Jar and install wget
RUN apk update && \
    apk add --no-cache wget && \
    wget https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop2-2.2.11/gcs-connector-hadoop2-2.2.11-shaded.jar -P /app/provider/scripts/spark/jars/ && \
    rm -rf /var/cache/apk/*

ENV SPARK_SCRIPT_PATH="/app/provider/scripts/spark/offline_store_spark_runner.py"
ENV PYTHON_INIT_PATH="/app/provider/scripts/spark/python_packages.sh"

EXPOSE 8080
ENTRYPOINT ["/app/main"]