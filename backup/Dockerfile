FROM golang:1.18-alpine

WORKDIR /app

COPY ../go.mod ./
COPY ../go.sum ./

ENV PATH /go/bin:$PATH

COPY backup/save/main.go ./backup/save/main.go

RUN go build ./backup/save/main.go

FROM golang:1.18-alpine

COPY --from=0 ./app/main ./main

EXPOSE 5432
ENTRYPOINT ["./main"]