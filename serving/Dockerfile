# docker build -f ./serving/Dockerfile . -t serving in /serving
# docker tag serving:latest featureformcom/serving:latest
# docker push featureformcom/serving:latest
FROM golang:1.21-alpine

WORKDIR /app

COPY go.mod ./
COPY go.sum ./

COPY ./metadata/proto/metadata.proto ./metadata/proto/metadata.proto
COPY ./proto/ ./proto/
RUN apk update && apk add protobuf-dev && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest && go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
ENV PATH /go/bin:$PATH
RUN protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative ./metadata/proto/metadata.proto
RUN protoc --go_out=. --go_opt=paths=source_relative     --go-grpc_out=. --go-grpc_opt=paths=source_relative     ./proto/serving.proto

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

FROM alpine

COPY --from=0 ./app/main ./main
COPY --from=0 ./app/provider/scripts/spark/offline_store_spark_runner.py /app/provider/scripts/spark/offline_store_spark_runner.py
COPY --from=0 ./app/provider/scripts/spark/python_packages.sh /app/provider/scripts/spark/python_packages.sh
COPY --from=0 ./app/provider/queries/materialize_no_ts.sql /app/provider/queries/materialize_no_ts.sql
COPY --from=0 ./app/provider/queries/materialize_ts.sql /app/provider/queries/materialize_ts.sql

ENV SPARK_SCRIPT_PATH="/app/provider/scripts/spark/offline_store_spark_runner.py"
ENV PYTHON_INIT_PATH="/app/provider/scripts/spark/python_packages.sh"
ENV MATERIALIZE_NO_TIMESTAMP_QUERY_PATH="/app/provider/queries/materialize_no_ts.sql"
ENV MATERIALIZE_TIMESTAMP_QUERY_PATH="/app/provider/queries/materialize_ts.sql"

ENV SERVING_PORT "8080"
EXPOSE 8080
ENTRYPOINT ["./main"]