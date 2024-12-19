#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

FROM golang:1.22-alpine
WORKDIR /app

COPY go.mod ./
COPY go.sum ./

ENV PATH /go/bin:$PATH
COPY ./logging/ ./logging/
COPY ./helpers/ ./helpers/
COPY ./config/ ./config/
COPY ./fferr ./fferr/
COPY ./metadata/proto/ ./metadata/proto/
COPY ./streamer_proxy/main.go /app/

RUN go build /app/main.go

FROM golang:1.22-alpine

COPY --from=0 ./app/main ./main

EXPOSE 8086
ENTRYPOINT ["./main"]