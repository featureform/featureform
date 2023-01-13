FROM golang:1.18-alpine

WORKDIR /app

COPY ./go.mod ./
COPY ./go.sum ./

ENV PATH /go/bin:$PATH

COPY etcd/main.go ./etcd/main.go

RUN go build ./etcd/main.go

FROM golang:1.18-alpine

COPY --from=0 ./app/main ./main

EXPOSE 5432
ENTRYPOINT ["./main"]