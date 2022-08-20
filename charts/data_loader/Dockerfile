FROM golang:1.17-alpine

WORKDIR /app

COPY ../../go.mod ./
COPY ../../go.sum ./

ENV PATH /go/bin:$PATH

COPY charts/data_loader/loader.go ./helpers/data_loader/main.go

RUN go build ./helpers/data_loader/main.go


FROM golang:1.17-alpine

COPY --from=0 ./app/main ./main

EXPOSE 5432
ENTRYPOINT ["./main"]