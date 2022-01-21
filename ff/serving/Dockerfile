FROM golang:1.16-alpine

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY *.go ./

RUN go build -o /metadata_service_example

EXPOSE 2112
EXPOSE 8080
EXPOSE 8181

CMD [ "/metadata_service_example" ]