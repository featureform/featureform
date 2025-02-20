#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

FROM golang:1.22-alpine as Builder
WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY ./metadata/proto/metadata.proto ./metadata/proto/metadata.proto
COPY ./scheduling/proto/scheduling.proto ./scheduling/proto/scheduling.proto
COPY ./proto/ ./proto/
RUN apk update && apk add protobuf-dev && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest && go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
ENV PATH /go/bin:$PATH
RUN protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative ./metadata/proto/metadata.proto \
    && protoc --go_out=. --go_opt=paths=source_relative     --go-grpc_out=. --go-grpc_opt=paths=source_relative     ./proto/serving.proto \
    && protoc --go_out=. --go_opt=paths=source_relative     --go-grpc_out=. --go-grpc_opt=paths=source_relative     ./scheduling/proto/scheduling.proto

COPY ./logging/ ./logging/
COPY ./filestore/ ./filestore/
COPY ./ffsync ./ffsync
COPY ./helpers/ ./helpers/
COPY ./config/ ./config/
COPY ./fferr ./fferr/
COPY ./lib/ ./lib/
COPY ./types/ ./types/
COPY ./provider/ ./provider/
COPY ./scheduling ./scheduling
COPY ./schema ./schema
COPY ./storage ./storage
COPY ./integrations/ ./integrations/
COPY ./metadata/proto/ ./metadata/proto/
COPY ./streamer_proxy/ ./streamer_proxy/
COPY ./metadata/ ./metadata/
COPY ./fftypes ./fftypes

RUN go build -o /app/main ./streamer_proxy/

FROM alpine:latest

COPY --from=builder ./app/main ./main

EXPOSE 8086
ENTRYPOINT ["./main"]
