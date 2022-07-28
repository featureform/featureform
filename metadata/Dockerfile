# RUN docker build -f ./metadata/Dockerfile . -t metadata from /serving
# docker tag metadata:latest featureformcom/metadata:latest
# docker push featureformcom/metadata:latest
FROM golang:1.17-alpine

WORKDIR /app

COPY go.mod ./
COPY go.sum ./

COPY ./metadata/proto/metadata.proto ./metadata/proto/metadata.proto
RUN apk update && apk add protobuf-dev && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest && go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
ENV PATH /go/bin:$PATH
RUN protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative ./metadata/proto/metadata.proto

COPY ./metadata/*.go ./metadata/
COPY ./metadata/proto/ ./metadata/proto/
COPY ./metadata/search/ ./metadata/search/
COPY ./metadata/server/server.go ./metadata/main/server.go

RUN go build ./metadata/main/server.go

FROM alpine

COPY --from=0 ./app/server /server

EXPOSE 8080
ENTRYPOINT ["./server"]