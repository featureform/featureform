FROM golang:1.22-alpine

WORKDIR /app

COPY ./go.mod ./
COPY ./go.sum ./

ENV PATH /go/bin:$PATH

COPY ./data_loader/loader.go ./helpers/data_loader/main.go

RUN go build ./helpers/data_loader/main.go

FROM golang:1.22-alpine

COPY --from=0 ./app/main ./main

EXPOSE 5432
ENTRYPOINT ["./main"]
