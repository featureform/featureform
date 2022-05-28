#!/bin/bash

docker buildx build -f ./api/Dockerfile . -t featureformcom/api-server:latest -o type=image --platform=linux/arm64,linux/amd64 --push &
docker buildx build -f ./coordinator/Dockerfile . -t featureformcom/coordinator:latest -o type=image --platform=linux/arm64,linux/amd64 --push &
docker buildx build -f ./metadata/Dockerfile . -t featureformcom/metadata:latest -o type=image --platform=linux/arm64,linux/amd64 --push &
docker buildx build -f ./metadata/dashboard/Dockerfile . -t featureformcom/metadata-dashboard:latest -o type=image --platform=linux/arm64,linux/amd64 --push &
docker buildx build -f ./newserving/Dockerfile . -t featureformcom/serving:latest -o type=image --platform=linux/arm64,linux/amd64 --push

