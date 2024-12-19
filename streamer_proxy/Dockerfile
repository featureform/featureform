FROM golang:1.21-alpine
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

FROM golang:1.21-alpine

COPY --from=0 ./app/main ./main

EXPOSE 8086
ENTRYPOINT ["./main"]