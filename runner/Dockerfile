# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
# docker build -f ./runner/Dockerfile . -t worker in /serving
# docker tag worker:latest featureformcom/worker:latest
# docker push featureformcom/worker:latest
FROM golang:1.18-alpine

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
#RUN go mod download

COPY ./metadata/proto/metadata.proto ./metadata/proto/metadata.proto
RUN apk update && apk add protobuf-dev && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest && go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
ENV PATH /go/bin:$PATH
RUN protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative ./metadata/proto/metadata.proto
COPY runner/*.go ./runner/
COPY runner/worker/*.go ./runner/worker/
COPY coordinator/*.go ./coordinator/
COPY helpers/ ./helpers/
COPY config/ ./config/
COPY logging/ ./logging/
COPY types/ ./types/
COPY kubernetes/ ./kubernetes/
COPY metadata/*.go ./metadata/
COPY metadata/search/* ./metadata/search/
COPY ./provider/ ./provider/
COPY runner/worker/main/main.go ./runner/worker/main/main.go

# CGD_ENABLED=0 is used to avoid the libc differences in alpine and ubuntu
ENV CGO_ENABLED=0
RUN go build -o worker ./runner/worker/main

FROM ubuntu:22.04

COPY --from=0 ./app/worker ./worker
COPY ./provider/scripts/spark/offline_store_spark_runner.py /app/provider/scripts/spark/offline_store_spark_runner.py
COPY ./provider/scripts/spark/python_packages.sh /app/provider/scripts/spark/python_packages.sh
COPY ./provider/scripts/spark/requirements.txt /app/provider/scripts/spark/requirements.txt

# Installing pyenv
RUN apt-get update && apt-get install -y \
    build-essential \
    checkinstall \
    libncursesw5-dev \
    libssl-dev \
    libsqlite3-dev \
    libgdbm-dev \
    libc6-dev \
    libbz2-dev \
    libffi-dev \
    zlib1g-dev \
    liblzma-dev \
    curl \
    git \
    openjdk-8-jdk \
    && rm -rf /var/lib/apt/lists/*

ENV ENV="/root/.bashrc"
ENV PYENV_ROOT="/.pyenv"
ENV PATH="$PYENV_ROOT/bin:$PATH"
RUN echo "PATH=${PATH}" > "${ENV}"

RUN curl https://pyenv.run | bash

# Install Python versions
RUN pyenv install 3.7.16 && pyenv global 3.7.16 && pyenv exec pip install --upgrade pip && pyenv exec pip install -r /app/provider/scripts/spark/requirements.txt
RUN pyenv install 3.8.16 && pyenv global 3.8.16 && pyenv exec pip install --upgrade pip && pyenv exec pip install -r /app/provider/scripts/spark/requirements.txt
RUN pyenv install 3.9.16 && pyenv global 3.9.16 && pyenv exec pip install --upgrade pip && pyenv exec pip install -r /app/provider/scripts/spark/requirements.txt
RUN pyenv install 3.10.10 && pyenv global 3.10.10 && pyenv exec pip install --upgrade pip && pyenv exec pip install -r /app/provider/scripts/spark/requirements.txt

ENV SPARK_SCRIPT_PATH="/app/provider/scripts/spark/offline_store_spark_runner.py"
ENV PYTHON_INIT_PATH="/app/provider/scripts/spark/python_packages.sh"

CMD [ "./worker" ]