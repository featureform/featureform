FROM golang:1.21 AS builder

WORKDIR /app

RUN GOBIN=/app go install github.com/pressly/goose/v3/cmd/goose@v3.23.0

FROM ubuntu:22.04

ENV GOOSE_DRIVER=postgres
ENV GOOSE_DBSTRING='postgres://postgres:password@localhost:5432/postgres?sslmode=disable'

WORKDIR /db

COPY --from=builder ./app/goose ./
COPY ./migrations ./migrations

ENTRYPOINT ["./goose", "up", "-dir", "./migrations"]