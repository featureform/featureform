FROM golang:1.21-alpine
WORKDIR /app

COPY go.mod ./
COPY go.sum ./
COPY ./streamer_proxy/proto/ ./streamer_proxy/proto/

RUN apk update && apk add protobuf-dev && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest && go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
ENV PATH /go/bin:$PATH
RUN protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative ./streamer_proxy/proto/streamer_proxy.proto

COPY ./streamer_proxy/main.go ./main.go

RUN go build ./main.go

FROM golang:1.21-alpine

RUN apk update && apk add --no-cache curl

COPY --from=0 ./app/main ./main

ENTRYPOINT ["./main"]