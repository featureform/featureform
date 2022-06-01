FROM golang:1.16-alpine as GO_BUILD
COPY . /server
WORKDIR /server/server
RUN go build -o /go/bin/server/server

FROM alpine:3.10
WORKDIR app
COPY --from=GO_BUILD /go/bin/server ./
COPY ./server/grpc ./grpc
EXPOSE 8080
CMD ./server