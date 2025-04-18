#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

# docker tag metadata:latest featureformcom/metadata:latest
# docker push featureformcom/metadata:latest
FROM golang:1.22-alpine

WORKDIR /app

COPY go.mod ./
COPY go.sum ./

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
COPY ./fftypes/ ./fftypes/
COPY ./storage ./storage
COPY ./scheduling ./scheduling
COPY ./schema ./schema
COPY ./lib/ ./lib/
COPY ./filestore/ ./filestore/
COPY ./logging/ ./logging/
COPY ./metadata/ ./metadata/
COPY ./db ./db
COPY ./helpers/ ./helpers/
COPY ./integrations/ ./integrations/
COPY ./provider/ ./provider
COPY ./config/ ./config/
COPY ./coordinator/types ./coordinator/types
COPY ./types/ ./types/
COPY ./kubernetes/ ./kubernetes/
COPY ./health/ ./health/

RUN go build ./metadata/server/server.go

FROM alpine

COPY --from=0 ./app/server /server
COPY ./db/migrations /db/migrations

EXPOSE 8080
ENTRYPOINT ["./server"]
