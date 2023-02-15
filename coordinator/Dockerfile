# RUN docker build -f ./coordinator/Dockerfile . -t coordinator from /serving
# docker tag coordinator:latest featureformcom/coordinator:latest
# docker push featureformcom/coordinator:latest
FROM golang:1.18-alpine

WORKDIR /app

COPY go.mod ./
COPY go.sum ./

COPY ./metadata/proto/metadata.proto ./metadata/proto/metadata.proto
RUN apk update && apk add protobuf-dev && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest && go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
ENV PATH /go/bin:$PATH
RUN protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative ./metadata/proto/metadata.proto

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

RUN go build ./coordinator/main/main.go

FROM alpine

COPY --from=0 ./app/main ./main
COPY ./provider/scripts/spark/offline_store_spark_runner.py /app/provider/scripts/spark/offline_store_spark_runner.py
COPY ./provider/scripts/spark/python_packages.sh /app/provider/scripts/spark/python_packages.sh
COPY ./provider/scripts/spark/requirements.txt /app/provider/scripts/spark/requirements.txt

# Installing pyenv
RUN apk --update add libffi-dev ncurses-dev openssl-dev readline-dev \
tk-dev xz-dev zlib-dev curl git bash build-base bzip2-dev sqlite-dev \
openjdk11 coreutils procps

ENV ENV=/root/.ashrc
ENV PYENV_ROOT='/.pyenv'
ENV PATH="$PYENV_ROOT/bin:/app/OME/.pyenv/versions/3.7.16/bin:$PATH"

RUN curl https://pyenv.run | bash

# Install Python versions
RUN pyenv install 3.7.16 && pyenv global 3.7.16 && pyenv exec pip install --upgrade pip && pyenv exec pip install -r /app/provider/scripts/spark/requirements.txt
RUN pyenv install 3.8.16 && pyenv global 3.8.16 && pyenv exec pip install --upgrade pip && pyenv exec pip install -r /app/provider/scripts/spark/requirements.txt
RUN pyenv install 3.9.16 && pyenv global 3.9.16 && pyenv exec pip install --upgrade pip && pyenv exec pip install -r /app/provider/scripts/spark/requirements.txt
RUN pyenv install 3.10.10 && pyenv global 3.10.10 && pyenv exec pip install --upgrade pip && pyenv exec pip install -r /app/provider/scripts/spark/requirements.txt

ENV SPARK_SCRIPT_PATH="/app/provider/scripts/spark/offline_store_spark_runner.py"
ENV PYTHON_INIT_PATH="/app/provider/scripts/spark/python_packages.sh"

EXPOSE 8080
ENTRYPOINT ["./main"]