# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
# docker build -f ./runner/Dockerfile . -t worker in /serving
# docker tag worker:latest featureformcom/worker:latest
# docker push featureformcom/worker:latest
FROM golang:1.21

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
#RUN go mod download

COPY ./metadata/proto/metadata.proto ./metadata/proto/metadata.proto
ENV PATH /go/bin:$PATH
RUN apt update && \
    apt install -y protobuf-compiler
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
RUN protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative ./metadata/proto/metadata.proto

COPY ./fferr ./fferr
COPY ./lib/ ./lib/
COPY ./filestore/ ./filestore/
COPY runner/*.go ./runner/
COPY runner/worker/*.go ./runner/worker/
COPY coordinator/*.go ./coordinator/
COPY helpers/ ./helpers/
COPY lib/ ./lib/
COPY config/ ./config/
COPY logging/ ./logging/
COPY types/ ./types/
COPY kubernetes/ ./kubernetes/
COPY metadata/*.go ./metadata/
COPY metadata/search/* ./metadata/search/
COPY ./provider/ ./provider/
COPY runner/worker/main/main.go ./runner/worker/main/main.go

ENV CGO_ENABLED=1
RUN go build -o worker ./runner/worker/main

FROM ubuntu:22.04

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
  wget \
  openjdk-8-jdk \
  && rm -rf /var/lib/apt/lists/*

# Download Shaded Jar
RUN wget https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop2-2.2.11/gcs-connector-hadoop2-2.2.11-shaded.jar -P /app/provider/scripts/spark/jars/

ENV ENV="/root/.bashrc"
ENV PYENV_ROOT="/.pyenv"
ENV PATH="$PYENV_ROOT/bin:$PATH"
RUN echo "PATH=${PATH}" > "${ENV}"

RUN curl https://pyenv.run | bash

COPY ./provider/scripts/spark/requirements.txt /app/provider/scripts/spark/requirements.txt
# Install Python versions

ARG TESTING
RUN if [ "$TESTING" = "True" ]; then \
  pyenv install 3.7.16 && pyenv global 3.7.16 && pyenv exec pip install --upgrade pip && pyenv exec pip install -r /app/provider/scripts/spark/requirements.txt ; \
  else \
  pyenv install 3.7.16 && pyenv global 3.7.16 && pyenv exec pip install --upgrade pip && pyenv exec pip install -r /app/provider/scripts/spark/requirements.txt && \
  pyenv install 3.8.16 && pyenv global 3.8.16 && pyenv exec pip install --upgrade pip && pyenv exec pip install -r /app/provider/scripts/spark/requirements.txt && \
  pyenv install 3.9.16 && pyenv global 3.9.16 && pyenv exec pip install --upgrade pip && pyenv exec pip install -r /app/provider/scripts/spark/requirements.txt && \
  pyenv install 3.10.10 && pyenv global 3.10.10 && pyenv exec pip install --upgrade pip && pyenv exec pip install -r /app/provider/scripts/spark/requirements.txt && \
  pyenv install 3.11.2 && pyenv global 3.11.2 && pyenv exec pip install --upgrade pip && pyenv exec pip install -r /app/provider/scripts/spark/requirements.txt ; \
  fi

ENV SPARK_SCRIPT_PATH="/app/provider/scripts/spark/offline_store_spark_runner.py"
ENV PYTHON_INIT_PATH="/app/provider/scripts/spark/python_packages.sh"
ENV MATERIALIZE_NO_TIMESTAMP_QUERY_PATH="/app/provider/queries/materialize_no_ts.sql"
ENV MATERIALIZE_TIMESTAMP_QUERY_PATH="/app/provider/queries/materialize_ts.sql"

COPY --from=0 ./app/worker ./worker
COPY ./provider/scripts/spark/offline_store_spark_runner.py /app/provider/scripts/spark/offline_store_spark_runner.py
COPY ./provider/scripts/spark/python_packages.sh /app/provider/scripts/spark/python_packages.sh
COPY ./provider/queries/materialize_no_ts.sql /app/provider/queries/materialize_no_ts.sql
COPY ./provider/queries/materialize_ts.sql /app/provider/queries/materialize_ts.sql


# Take the MD5 hash of the Spark runner script and store it in a file for use by the config package
# when determining the remove filepath in cloud object storage (e.g. S3). By adding the hash as a suffix
# to the file, we ensure that different versions of the script are uploaded to cloud object storage
# without overwriting previous or future versions.
RUN cat $SPARK_SCRIPT_PATH | md5sum \
  | awk '{print $1}' \
  | xargs echo -n > /app/provider/scripts/spark/offline_store_spark_runner_md5.txt


CMD [ "./worker" ]
