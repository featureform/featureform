# docker tag coordinator:latest featureformcom/coordinator:latest
# docker push featureformcom/coordinator:latest
FROM golang:1.22 as builder

WORKDIR /app

COPY go.mod go.sum ./

# Installing protobuf and compiling metadata
RUN apt update && \
  apt install -y protobuf-compiler
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

COPY ./metadata/proto/metadata.proto ./metadata/proto/
COPY ./scheduling/proto/scheduling.proto ./scheduling/proto/scheduling.proto
COPY ./proto/ ./proto/

ENV PATH /go/bin:$PATH
RUN protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative ./metadata/proto/metadata.proto \
  && protoc --go_out=. --go_opt=paths=source_relative     --go-grpc_out=. --go-grpc_opt=paths=source_relative     ./proto/serving.proto \
  && protoc --go_out=. --go_opt=paths=source_relative     --go-grpc_out=. --go-grpc_opt=paths=source_relative     ./scheduling/proto/scheduling.proto

# Copying source files
COPY ./fferr ./fferr
COPY ./ffsync ./ffsync
COPY ./fftypes/ ./fftypes/
COPY ./storage ./storage
COPY ./scheduling ./scheduling
COPY ./schema ./schema
COPY ./lib/ ./lib/
COPY ./filestore/ ./filestore/
COPY ./integrations/ ./integrations/
COPY ./coordinator/ ./coordinator/
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
ENV CGO_ENABLED=1
RUN go build ./coordinator/main/main.go

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
  openjdk-8-jdk \
  curl \
  git \
  wget \
  && rm -rf /var/lib/apt/lists/*

## Download Shaded Jar
RUN wget https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop2-2.2.11/gcs-connector-hadoop2-2.2.11-shaded.jar -P /app/provider/scripts/spark/jars/

ENV ENV="/root/.bashrc"
ENV PYENV_ROOT="/.pyenv"
ENV PATH="$PYENV_ROOT/bin:$PATH"
RUN echo "PATH=${PATH}" > "${ENV}"

RUN curl https://pyenv.run | bash

ARG SPARK_FILEPATH=/app/provider/scripts/spark/offline_store_spark_runner.py
ARG SPARK_PYTHON_PACKAGES=/app/provider/scripts/spark/python_packages.sh
ARG SPARK_REQUIREMENTS=/app/provider/scripts/spark/requirements.txt
ARG MATERIALIZE_NO_TIMESTAMP_QUERY_PATH=/app/provider/queries/materialize_no_ts.sql
ARG MATERIALIZE_TIMESTAMP_QUERY_PATH=/app/provider/queries/materialize_ts.sql

COPY provider/scripts/spark/offline_store_spark_runner.py $SPARK_FILEPATH
COPY provider/scripts/spark/python_packages.sh $SPARK_PYTHON_PACKAGES
COPY provider/scripts/spark/requirements.txt $SPARK_REQUIREMENTS
COPY provider/queries/materialize_no_ts.sql $MATERIALIZE_NO_TIMESTAMP_QUERY_PATH
COPY provider/queries/materialize_ts.sql $MATERIALIZE_TIMESTAMP_QUERY_PATH

ENV SPARK_SCRIPT_PATH=$SPARK_FILEPATH
ENV PYTHON_INIT_PATH=$SPARK_PYTHON_PACKAGES
ENV MATERIALIZE_NO_TIMESTAMP_QUERY_PATH=$MATERIALIZE_NO_TIMESTAMP_QUERY_PATH
ENV MATERIALIZE_TIMESTAMP_QUERY_PATH=$MATERIALIZE_TIMESTAMP_QUERY_PATH
ENV SPARK_REQUIREMENTS=$SPARK_REQUIREMENTS

COPY ./provider/scripts/spark/requirements.txt /app/provider/scripts/spark/requirements.txt

# TODO: (Sterling) Add a better solution to this
### Install Python versions
#ARG TESTING
#RUN if [ "$TESTING" = "True" ]; then \
#  pyenv install 3.7.16 && pyenv global 3.7.16 && pyenv exec pip install --upgrade pip && pyenv exec pip install -r $SPARK_REQUIREMENTS ; \
#  else \
#  pyenv install 3.7.16 && pyenv global 3.7.16 && pyenv exec pip install --upgrade pip && pyenv exec pip install -r $SPARK_REQUIREMENTS && \
#  pyenv install 3.8.16 && pyenv global 3.8.16 && pyenv exec pip install --upgrade pip && pyenv exec pip install -r $SPARK_REQUIREMENTS && \
#  pyenv install 3.9.16 && pyenv global 3.9.16 && pyenv exec pip install --upgrade pip && pyenv exec pip install -r $SPARK_REQUIREMENTS && \
#  pyenv install 3.10.10 && pyenv global 3.10.10 && pyenv exec pip install --upgrade pip && pyenv exec pip install -r $SPARK_REQUIREMENTS && \
#  pyenv install 3.11.2 && pyenv global 3.11.2 && pyenv exec pip install --upgrade pip && pyenv exec pip install -r $SPARK_REQUIREMENTS ; \
#  fi


COPY --from=builder ./app/main /app/main
COPY ./provider/queries/ /app/provider/queries/
COPY ./provider/scripts/spark/ /app/provider/scripts/spark/

# Take the MD5 hash of the Spark runner script and store it in a file for use by the config package
# when determining the remove filepath in cloud object storage (e.g. S3). By adding the hash as a suffix
# to the file, we ensure that different versions of the script are uploaded to cloud object storage
# without overwriting previous or future versions.
RUN cat $SPARK_SCRIPT_PATH | md5sum \
  | awk '{print $1}' \
  | xargs echo -n > /app/provider/scripts/spark/offline_store_spark_runner_md5.txt


EXPOSE 8080
ENTRYPOINT ["/app/main"]
