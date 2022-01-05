FROM golang:1.16-alpine

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY *.go ./

RUN go build -o /metrics_service_example

EXPOSE 2112
EXPOSE 8080

CMD [ "/metrics_service_example" ]