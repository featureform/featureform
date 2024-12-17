FROM golang:1.21-alpine
WORKDIR /app

COPY go.mod ./
COPY go.sum ./

ENV PATH /go/bin:$PATH
COPY ./streamer_proxy/main.go ./main.go
COPY ./streamer_proxy/iceberg_proxy.go ./iceberg_proxy.go

RUN go build ./main.go

FROM golang:1.21-alpine

RUN apk update && apk add --no-cache curl

COPY --from=0 ./app/main ./main
EXPOSE 8086

ENTRYPOINT ["./main"]