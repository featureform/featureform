# RUN docker build -f ./coordinator/Dockerfile . -t coordinator from /serving
# docker tag coordinator:latest featureformcom/coordinator:latest
# docker push featureformcom/coordinator:latest
FROM golang:1.17-alpine

WORKDIR /app

COPY go.mod ./
COPY go.sum ./

COPY ./metadata/proto/metadata.proto ./metadata/proto/metadata.proto
RUN apk update && apk add protobuf-dev && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest && go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
ENV PATH /go/bin:$PATH
RUN protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative ./metadata/proto/metadata.proto

COPY ./coordinator/*.go ./coordinator/
COPY ./provider/ ./provider/
COPY ./metadata/ ./metadata/
COPY ./runner/ ./runner/
COPY ./coordinator/main/main.go ./coordinator/main/main.go

RUN go build ./coordinator/main/main.go

FROM alpine

COPY --from=0 ./app/main ./main

EXPOSE 8080
ENTRYPOINT ["./main"]