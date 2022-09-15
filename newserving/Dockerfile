# docker build -f ./newserving/Dockerfile . -t serving in /serving
# docker tag serving:latest featureformcom/serving:latest
# docker push featureformcom/serving:latest
FROM golang:1.17-alpine

WORKDIR /app

COPY go.mod ./
COPY go.sum ./

COPY ./metadata/proto/metadata.proto ./metadata/proto/metadata.proto
COPY ./proto/ ./proto/
RUN apk update && apk add protobuf-dev && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest && go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
ENV PATH /go/bin:$PATH
RUN protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative ./metadata/proto/metadata.proto
RUN protoc --go_out=. --go_opt=paths=source_relative     --go-grpc_out=. --go-grpc_opt=paths=source_relative     ./proto/serving.proto

COPY newserving/*.go ./newserving/
COPY ./metadata/ ./metadata/
COPY ./helpers/ ./helpers/
COPY ./metrics/ ./metrics/
COPY ./provider/ ./provider/
COPY newserving/main/main.go ./newserving/main/main.go

RUN go build ./newserving/main/main.go

FROM alpine

COPY --from=0 ./app/main ./main

ENV SERVING_PORT "0.0.0.0:8080"
EXPOSE 8080
ENTRYPOINT ["./main"]