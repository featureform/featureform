FROM golang:1.22-alpine

WORKDIR /app

COPY ./go.mod ./
COPY ./go.sum ./

COPY ./metadata/proto/metadata.proto ./metadata/proto/metadata.proto
COPY ./scheduling/proto/scheduling.proto ./scheduling/proto/scheduling.proto
COPY ./proto/ ./proto/
RUN apk update && apk add protobuf-dev && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest && go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
ENV PATH /go/bin:$PATH
RUN protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative ./metadata/proto/metadata.proto \
    && protoc --go_out=. --go_opt=paths=source_relative     --go-grpc_out=. --go-grpc_opt=paths=source_relative     ./proto/serving.proto \
    && protoc --go_out=. --go_opt=paths=source_relative     --go-grpc_out=. --go-grpc_opt=paths=source_relative     ./scheduling/proto/scheduling.proto

COPY ./fferr ./fferr
COPY ./ffsync ./ffsync
COPY ./scheduling ./scheduling
COPY ./schema ./schema
COPY ./storage ./storage
COPY ./filestore/ ./filestore/
COPY backup/ ./backup/
COPY helpers/ ./helpers/
COPY integrations/ ./integrations/
COPY lib/ ./lib/
COPY provider/ ./provider/
COPY config/ ./config/
COPY kubernetes/ ./kubernetes/
COPY logging/ ./logging/
COPY metadata/ ./metadata/
COPY metadata/proto/ ./metadata/proto/
COPY types/ ./types/
COPY proto/ ./proto/

RUN go build ./backup/save/main.go 

FROM golang:1.22-alpine

COPY --from=0 ./app/main ./main

ENTRYPOINT ["./main"]
