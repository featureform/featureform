# docker build -f ./serving/Dockerfile . -t serving in /serving
# docker tag serving:latest featureformcom/serving:latest
# docker push featureformcom/serving:latest
FROM golang:1.21

WORKDIR /app

COPY go.mod ./
COPY go.sum ./

COPY ./metadata/proto/metadata.proto ./metadata/proto/metadata.proto
COPY ./proto/ ./proto/
ENV PATH /go/bin:$PATH
RUN apt update && \
    apt install -y protobuf-compiler
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
RUN protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative ./metadata/proto/metadata.proto && \
    protoc --go_out=. --go_opt=paths=source_relative     --go-grpc_out=. --go-grpc_opt=paths=source_relative     ./proto/serving.proto

COPY ./fferr ./fferr
COPY ./lib/ ./lib/
COPY ./filestore/ ./filestore/
COPY serving/*.go ./serving/
COPY ./metadata/ ./metadata/
COPY ./config/ ./config/
COPY ./helpers/ ./helpers/
COPY ./logging/ ./logging/
COPY ./types/ ./types/
COPY ./kubernetes/ ./kubernetes/
COPY ./metrics/ ./metrics/
COPY ./provider/ ./provider/
COPY serving/main/main.go ./serving/main/main.go

RUN go build ./serving/main/main.go

FROM ubuntu:22.04

COPY --from=0 ./app/main ./main
COPY --from=0 ./app/provider/scripts/spark/offline_store_spark_runner.py /app/provider/scripts/spark/offline_store_spark_runner.py
COPY --from=0 ./app/provider/scripts/spark/python_packages.sh /app/provider/scripts/spark/python_packages.sh
COPY --from=0 ./app/provider/queries/materialize_no_ts.sql /app/provider/queries/materialize_no_ts.sql
COPY --from=0 ./app/provider/queries/materialize_ts.sql /app/provider/queries/materialize_ts.sql

ENV SPARK_SCRIPT_PATH="/app/provider/scripts/spark/offline_store_spark_runner.py"
ENV PYTHON_INIT_PATH="/app/provider/scripts/spark/python_packages.sh"
ENV MATERIALIZE_NO_TIMESTAMP_QUERY_PATH="/app/provider/queries/materialize_no_ts.sql"
ENV MATERIALIZE_TIMESTAMP_QUERY_PATH="/app/provider/queries/materialize_ts.sql"

# Take the MD5 hash of the Spark runner script and store it in a file for use by the config package
# when determining the remove filepath in cloud object storage (e.g. S3). By adding the hash as a suffix
# to the file, we ensure that different versions of the script are uploaded to cloud object storage
# without overwriting previous or future versions.
RUN cat $SPARK_SCRIPT_PATH | md5sum \
    | awk '{print $1}' \
    | xargs echo -n > /app/provider/scripts/spark/offline_store_spark_runner_md5.txt

ENV SERVING_PORT "8080"
EXPOSE 8080
ENTRYPOINT ["./main"]