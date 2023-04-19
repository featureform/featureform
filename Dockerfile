# Build dashboard
FROM node:16-alpine as dashboard-builder
COPY ./dashboard ./dashboard
WORKDIR ./dashboard
RUN npm install --legacy-peer-deps
RUN npm run build
RUN rm -r node_modules

# Build Go services
FROM golang:1.18 as go-builder

WORKDIR /app

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
COPY serving/ serving/
COPY types/ types/
COPY kubernetes/ kubernetes/
COPY config/ config/
COPY logging/ logging/

RUN apt install protobuf-compiler -y
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
RUN protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative ./proto/serving.proto
RUN protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative ./metadata/proto/metadata.proto

RUN mkdir execs
RUN go build -o execs/api api/main.go
RUN go build -o execs/metadata metadata/server/server.go
RUN go build -o execs/coordinator coordinator/main/main.go
RUN go build -o execs/dashboard_metadata metadata/dashboard/dashboard_metadata.go
RUN go build -o execs/serving serving/main/main.go

# Final image
FROM golang:1.18

WORKDIR /app

# Copy built Go services
COPY --from=go-builder /app/execs /app/execs

# Copy built dashboard
COPY --from=dashboard-builder ./dashboard ./dashboard

# Install and configure Supervisor
RUN apt-get update && apt-get install -y supervisor
RUN mkdir -p /var/lock/apache2 /var/run/apache2 /var/run/sshd /var/log/supervisor
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Install Nginx
RUN apt-get update
RUN apt-get install -y nginx --option=Dpkg::Options::=--force-confdef
COPY nginx.conf /etc/nginx/nginx.conf

# Install MeiliSearch
RUN curl -L https://install.meilisearch.com | sh

ENV SERVING_PORT="8082"
ENV SERVING_HOST="0.0.0.0"
ENV ETCD_ARCH=""

EXPOSE 7878
CMD ["/usr/bin/supervisord"]