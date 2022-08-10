# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
# docker build -f ./runner/Dockerfile . -t worker in /serving
# docker tag worker:latest featureformcom/worker:latest
# docker push featureformcom/worker:latest
FROM golang:1.17-alpine

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY ./metadata/proto/metadata.proto ./metadata/proto/metadata.proto
RUN apk update && apk add protobuf-dev && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest && go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
ENV PATH /go/bin:$PATH
RUN protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative ./metadata/proto/metadata.proto
COPY runner/*.go ./runner/
COPY runner/worker/*.go ./runner/worker/
COPY coordinator/*.go ./coordinator/
COPY metadata/*.go ./metadata/
COPY metadata/search/* ./metadata/search/
COPY provider/* ./provider/
COPY runner/worker/main/main.go ./runner/worker/main/main.go

RUN go build -o worker ./runner/worker/main

FROM alpine

COPY --from=0 ./app/worker ./worker

CMD [ "./worker" ]