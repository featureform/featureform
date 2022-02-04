#!/bin/bash
kubectl create namespace test-1
kubectl apply -f rbac.yaml -n test-1
kubectl apply -f config-map.yaml -n test-1
kubectl apply -f prometheus-deployment.yaml -n test-1
kubectl apply -f prometheus-service.yaml -n test-1
