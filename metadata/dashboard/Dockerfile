# docker buildx build -f ./metadata/dashboard/Dockerfile . -t featureformcom/metadata-dashboard:latest -o type=image --platform=linux/arm64,linux/amd64 --push
FROM golang:1.17-alpine

WORKDIR /app

COPY go.mod ./
COPY go.sum ./

COPY ./metadata/proto/metadata.proto ./metadata/proto/metadata.proto
RUN apk update && apk add protobuf-dev && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest && go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
ENV PATH /go/bin:$PATH
RUN protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative ./metadata/proto/metadata.proto

COPY ./metadata/*.go ./metadata/
COPY ./metadata/dashboard/ ./metadata/dashboard/
COPY ./metadata/search/ ./metadata/search/

RUN go build ./metadata/dashboard/dashboard_metadata.go

FROM alpine

COPY --from=0 ./app/dashboard_metadata ./dashboard_metadata

EXPOSE 8080
ENTRYPOINT ["./dashboard_metadata"]