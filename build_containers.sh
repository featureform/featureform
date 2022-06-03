#!/bin/bash

docker buildx d -o type=image --platform=linux/arm64,linux/amd64 --push &
docker buildx build -f ./coordinator/Dockerfile . -t featureformcom/coordinator:stable -o type=image --platform=linux/arm64,linux/amd64 --push &
docker buildx build -f ./metadata/Dockerfile . -t featureformcom/metadata:stable -o type=image --platform=linux/arm64,linux/amd64 --push &
docker buildx build -f ./metadata/dashboard/Dockerfile . -t featureformcom/metadata-dashboard:stable -o type=image --platform=linux/arm64,linux/amd64 --push &
docker buildx build -f ./newserving/Dockerfile . -t featureformcom/serving:stable -o type=image --platform=linux/arm64,linux/amd64 --push &
docker buildx build -f ./runner/Dockerfile . -t featureformcom/worker:stable -o type=image --platform=linux/arm64,linux/amd64 --push

