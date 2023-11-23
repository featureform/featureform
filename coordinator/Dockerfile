# RUN docker build -f ./coordinator/Dockerfile . -t coordinator from /serving
# docker tag coordinator:latest featureformcom/coordinator:latest
# docker push featureformcom/coordinator:latest
FROM golang:1.18-alpine as builder

WORKDIR /app

COPY go.mod go.sum ./

# Installing protobuf and compiling metadata
RUN apk update && apk add protobuf-dev && \
  go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest && \
  go install google.golang.org/protobuf/cmd/protoc-gen-go@latest && \
  rm -rf /var/cache/apk/*

COPY ./metadata/proto/metadata.proto ./metadata/proto/
ENV PATH /go/bin:$PATH
RUN protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative ./metadata/proto/metadata.proto

# Copying source files
COPY ./lib/ ./lib/
COPY ./filestore/ ./filestore/
COPY ./coordinator/*.go ./coordinator/
COPY ./provider/ ./provider/
COPY ./config/ ./config/
COPY ./helpers/ ./helpers/
COPY ./logging/ ./logging/
COPY ./metadata/ ./metadata/
COPY ./runner/ ./runner/
COPY ./kubernetes ./kubernetes
COPY ./types ./types
COPY ./coordinator/main/main.go ./coordinator/main/main.go

# Building Go app
# CGD_ENABLED=0 is used to avoid the libc differences in alpine and ubuntu
ENV CGO_ENABLED=0
RUN go build ./coordinator/main/main.go

FROM ubuntu:22.04

COPY --from=builder ./app/main /app/main
COPY ./provider/scripts/spark/ /app/provider/scripts/spark/

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
  openjdk-8-jdk \
  curl \
  git \
  wget \
  && rm -rf /var/lib/apt/lists/*

ENV ENV="/root/.bashrc"
ENV PYENV_ROOT="/.pyenv"
ENV PATH="$PYENV_ROOT/bin:$PATH"
RUN echo "PATH=${PATH}" > "${ENV}"

RUN curl https://pyenv.run | bash

## Install Python versions
#ARG TESTING
#RUN if [ "$TESTING" = "True" ]; then \
#  pyenv install 3.7.16 && pyenv global 3.7.16 && pyenv exec pip install --upgrade pip && pyenv exec pip install -r /app/provider/scripts/spark/requirements.txt ; \
#  else \
#  pyenv install 3.7.16 && pyenv global 3.7.16 && pyenv exec pip install --upgrade pip && pyenv exec pip install -r /app/provider/scripts/spark/requirements.txt && \
#  pyenv install 3.8.16 && pyenv global 3.8.16 && pyenv exec pip install --upgrade pip && pyenv exec pip install -r /app/provider/scripts/spark/requirements.txt && \
#  pyenv install 3.9.16 && pyenv global 3.9.16 && pyenv exec pip install --upgrade pip && pyenv exec pip install -r /app/provider/scripts/spark/requirements.txt && \
#  pyenv install 3.10.10 && pyenv global 3.10.10 && pyenv exec pip install --upgrade pip && pyenv exec pip install -r /app/provider/scripts/spark/requirements.txt && \
#  pyenv install 3.11.2 && pyenv global 3.11.2 && pyenv exec pip install --upgrade pip && pyenv exec pip install -r /app/provider/scripts/spark/requirements.txt ; \
#  fi

ENV SPARK_SCRIPT_PATH="/app/provider/scripts/spark/offline_store_spark_runner.py"
ENV PYTHON_INIT_PATH="/app/provider/scripts/spark/python_packages.sh"
ENV MATERIALIZE_NO_TIMESTAMP_QUERY_PATH="/app/provider/queries/materialize_no_ts.sql"
ENV MATERIALIZE_TIMESTAMP_QUERY_PATH="/app/provider/queries/materialize_ts.sql"

## Download Shaded Jar
#RUN wget https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop2-2.2.11/gcs-connector-hadoop2-2.2.11-shaded.jar -P /app/provider/scripts/spark/jars/

EXPOSE 8080
ENTRYPOINT ["/app/main"]