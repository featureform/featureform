#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

FROM golang:1.21-alpine as Builder
WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

ENV PATH /go/bin:$PATH
COPY ./logging/ ./logging/
COPY ./helpers/ ./helpers/
COPY ./config/ ./config/
COPY ./fferr ./fferr/
COPY ./metadata/proto/ ./metadata/proto/
COPY ./streamer_proxy/ ./streamer_proxy/

RUN go build -o /app/main ./streamer_proxy/

FROM alpine:latest

COPY --from=builder ./app/main ./main

EXPOSE 8086
ENTRYPOINT ["./main"]
