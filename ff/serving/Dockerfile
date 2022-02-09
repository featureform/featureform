FROM golang:1.16-alpine

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
COPY proto ./proto
COPY dataset ./dataset
COPY testdata ./testdata
COPY postgres ./postgres
RUN go mod download

COPY *.go ./

RUN go build -o /feature_server

EXPOSE 2112
EXPOSE 8080

CMD [ "/feature_server" ]