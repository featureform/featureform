# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

gen_grpc:
	pip install grpcio-tools

	-mkdir client/src/featureform/proto/
	set -e
	cp metadata/proto/metadata.proto client/src/featureform/proto/metadata.proto
	cp proto/serving.proto client/src/featureform/proto/serving.proto

	protoc --go_out=. --go_opt=paths=source_relative     --go-grpc_out=. --go-grpc_opt=paths=source_relative     ./proto/serving.proto
	python -m grpc_tools.protoc -I ./client/src --python_out=./client/src --grpc_python_out=./client/src/ ./client/src/featureform/proto/serving.proto

	protoc --go_out=. --go_opt=paths=source_relative     --go-grpc_out=. --go-grpc_opt=paths=source_relative     ./metadata/proto/metadata.proto
	python -m grpc_tools.protoc -I ./client/src --python_out=./client/src/ --grpc_python_out=./client/src/ ./client/src/featureform/proto/metadata.proto

update_python: gen_grpc
	pip install build
	pip uninstall featureform  -y
	rm -r client/dist/*
	python3 -m build ./client/
	pip install client/dist/*.whl


build_containers:
	docker build -f ./api/Dockerfile . -t local/api-server:stable & \
	docker build -f ./dashboard/Dockerfile . -t local/dashboard:stable & \
	docker build -f ./coordinator/Dockerfile . -t local/coordinator:stable & \
	docker build -f ./metadata/Dockerfile . -t local/metadata:stable & \
	docker build -f ./metadata/dashboard/Dockerfile . -t local/metadata-dashboard:stable & \
	docker build -f ./newserving/Dockerfile . -t local/serving:stable & \
	wait; \
	echo "Build Complete"
	minikube image load local/api-server:stable
	minikube image load local/dashboard:stable
	minikube image load local/coordinator:stable
	minikube image load local/metadata:stable
	minikube image load local/metadata-dashboard:stable
	minikube image load local/serving:stable

load_containers: build_containers
	minikube image load local/api-server:stable
#	minikube image load local/coordinator:stable & \
#	minikube image load local/dashboard:stable & \
#	minikube image load local/metadata-dashboard:stable & \
#	minikube image load local/metadata:stable & \
#	minikube image load local/serving:stable & \
#	minikube image load local/worker:stable & \


start_minikube:
	minikube start

reset_minikube:
	minikube delete
	minikube start

#install_featureform:
#	helm install featureform ./charts/featureform
#
#install_quickstart:
#	helm install quickstart ./charts/quickstart
#
#update_featureform:
#	helm upgrade featureform ./charts/featureform
#	helm upgrade quickstart ./charts/quickstart

uninstall_featureform:
	helm uninstall featureform

uninstall_quickstart:
	helm uninstall quickstart

reset_quickstart: uninstall_quickstart install_quickstart

reset_featureform: uninstall_featureform install_featureform

install_featureform: start_minikube load_containers
	helm repo add jetstack https://charts.jetstack.io
	helm repo update
	helm install certmgr jetstack/cert-manager \
        --set installCRDs=true \
        --version v1.8.0 \
        --namespace cert-manager \
        --create-namespace
	helm install featureform ./charts/featureform --set global.repo=local --set global.pullPolicy=Never --set global.version=stable
	kubectl get secret featureform-ca-secret -o=custom-columns=':.data.tls\.crt'| base64 -d > tls.crt
	minikube addons enable ingress
	minikube tunnel
	export FEATUREFORM_HOST="localhost:443"
    export FEATUREFORM_CERT="tls.crt"

update_featureform:  load_containers
	helm upgrade featureform ./charts/featureform --set global.repo=local --set global.pullPolicy=Never --set global.version=stable
	kubectl get secret featureform-ca-secret -o=custom-columns=':.data.tls\.crt'| base64 -d > tls.crt
	minikube addons enable ingress
	minikube tunnel
	export FEATUREFORM_HOST="localhost:443"
    export FEATUREFORM_CERT="tls.crt"


