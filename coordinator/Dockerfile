# RUN docker build -f ./coordinator/Dockerfile . -t coordinator from /serving
# docker tag coordinator:latest featureformcom/coordinator:latest
# docker push featureformcom/coordinator:latest
FROM golang:1.18-alpine

WORKDIR /app

COPY go.mod ./
COPY go.sum ./

COPY ./metadata/proto/metadata.proto ./metadata/proto/metadata.proto
RUN apk update && apk add protobuf-dev && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest && go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
ENV PATH /go/bin:$PATH
RUN protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative ./metadata/proto/metadata.proto

COPY ./filestore/ ./filestore/
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

# CGD_ENABLED=0 is used to avoid the libc differences in alpine and ubuntu
ENV CGO_ENABLED=0
RUN go build ./coordinator/main/main.go

FROM featureformcom/coordinator_base:latest

COPY --from=0 ./app/main /app/main

# Download Shaded Jar
RUN apt-get install -y wget
RUN wget https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop2-2.2.11/gcs-connector-hadoop2-2.2.11-shaded.jar -P /app/provider/scripts/spark/jars/

ENV SPARK_SCRIPT_PATH="/app/provider/scripts/spark/offline_store_spark_runner.py"
ENV PYTHON_INIT_PATH="/app/provider/scripts/spark/python_packages.sh"

EXPOSE 8080
ENTRYPOINT ["/app/main"]