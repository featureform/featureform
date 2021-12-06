FROM golang:1.16-alpine

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY *.go ./

RUN go build -o /metrics-test-service

EXPOSE 2112

CMD [ "/metrics-test-service" ]