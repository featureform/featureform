FROM node:16-alpine
COPY ./dashboard ./dashboard
WORKDIR ./dashboard
RUN npm install --legacy-peer-deps
RUN npm run build
RUN rm -r node_modules

FROM golang:1.17

WORKDIR /app

COPY --from=0 ./dashboard ./dashboard

RUN apt-get update && apt-get install -y supervisor
RUN mkdir -p /var/lock/apache2 /var/run/apache2 /var/run/sshd /var/log/supervisor

COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf
COPY go.mod ./
COPY go.sum ./
COPY api/ api/
COPY helpers/ helpers/
COPY metadata/ metadata/
COPY metrics/ metrics/
COPY proto/ proto/
COPY coordinator/ coordinator/
COPY provider/ provider/
COPY runner/ runner/
COPY newserving/ newserving/
COPY nginx.conf/ /etc/nginx/nginx.conf

RUN apt install protobuf-compiler -y
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
RUN protoc --go_out=. --go_opt=paths=source_relative     --go-grpc_out=. --go-grpc_opt=paths=source_relative     ./proto/serving.proto
RUN protoc --go_out=. --go_opt=paths=source_relative     --go-grpc_out=. --go-grpc_opt=paths=source_relative     ./metadata/proto/metadata.proto

RUN mkdir execs
RUN go build api/main.go
RUN mv main execs/api
RUN go build metadata/server/server.go
RUN mv server execs/metadata
RUN go build coordinator/main/main.go
RUN mv main execs/coordinator
RUN go build metadata/dashboard/dashboard_metadata.go
RUN mv dashboard_metadata execs/dashboard_metadata
RUN go build newserving/main/main.go
RUN mv main execs/serving

RUN git clone -b v3.4.16 https://github.com/etcd-io/etcd.git
WORKDIR /app/etcd
RUN ./build
WORKDIR /app
RUN ETCD_UNSUPPORTED_ARCH=arm64 ./etcd/bin/etcd --version

RUN apt-get update
RUN apt-get install -y nginx --option=Dpkg::Options::=--force-confdef

WORKDIR /app

ENV ENABLE_TYPESENSE="false"
ENV SERVING_PORT="8082"
ENV SERVING_HOST="0.0.0.0"
ENV ETCD_ARCH=""

EXPOSE 7878
CMD ["/usr/bin/supervisord"]