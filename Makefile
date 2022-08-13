# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

## ----------------------------------------------------------------------
## To run End-To-End Tests Run:
## make etcdctl
## make update_python
## make start_minikube
## make containers
## make test_e2e
## ....
## When updating a file and rerunning the test:
## make reset_e2e
## make update_python
## 	or
## make containers
## 	then
## make test_e2e
## ----------------------------------------------------------------------

help:     						## Show this help.
	@sed -ne '/@sed/!s/## //p' $(MAKEFILE_LIST)

pytest:
	-rm -r .featureform
	curl -C - https://featureform-demo-files.s3.amazonaws.com/transactions.csv -o transactions.csv
	pytest client/src/featureform/local_dash_test.py
	pytest client/tests/redefined_test.py
	pytest client/tests/local_test.py
	pytest client/tests/serving_test.py
	pytest client/tests/localmode_quickstart_test.py
	pip install jupyter nbconvert matplotlib pandas scikit-learn requests
	jupyter nbconvert --to notebook --execute notebooks/Fraud_Detection_Example.ipynb
	-rm -r .featureform

etcdctl: 						## Installs ETCDCTL. Required for reset_e2e
	-git clone -b v3.4.16 https://github.com/etcd-io/etcd.git
	cd etcd && ./build
	export PATH=$PATH:"`pwd`/etcd/bin"
	etcdctl version

gen_grpc:						## Generates GRPC Dependencies
	pip3 install grpcio-tools

	-mkdir client/src/featureform/proto/
	set -e
	cp metadata/proto/metadata.proto client/src/featureform/proto/metadata.proto
	cp proto/serving.proto client/src/featureform/proto/serving.proto

	protoc --go_out=. --go_opt=paths=source_relative     --go-grpc_out=. --go-grpc_opt=paths=source_relative     ./proto/serving.proto
	python -m grpc_tools.protoc -I ./client/src --python_out=./client/src --grpc_python_out=./client/src/ ./client/src/featureform/proto/serving.proto

	protoc --go_out=. --go_opt=paths=source_relative     --go-grpc_out=. --go-grpc_opt=paths=source_relative     ./metadata/proto/metadata.proto
	python -m grpc_tools.protoc -I ./client/src --python_out=./client/src/ --grpc_python_out=./client/src/ ./client/src/featureform/proto/metadata.proto

update_python: gen_grpc 			## Updates the python package locally
	pip3 install pytest
	pip3 install build
	pip3 uninstall featureform  -y
	-rm -r client/dist/*
	python3 -m build ./client/
	pip3 install client/dist/*.whl


containers:						## Build Docker containers for Minikube
	minikube image build -f ./api/Dockerfile . -t local/api-server:stable & \
	minikube image build -f ./dashboard/Dockerfile . -t local/dashboard:stable & \
	minikube image build -f ./coordinator/Dockerfile . -t local/coordinator:stable & \
	minikube image build -f ./metadata/Dockerfile . -t local/metadata:stable & \
	minikube image build -f ./metadata/dashboard/Dockerfile . -t local/metadata-dashboard:stable & \
	minikube image build -f ./newserving/Dockerfile . -t local/serving:stable & \
	wait; \
	echo "Build Complete"

start_minikube:	##Starts Minikube
	minikube start

reset_minikube:	##Resets Minikube
	minikube delete
	minikube start

install_featureform: start_minikube containers		## Configures Featureform on Minikube
	helm repo add jetstack https://charts.jetstack.io
	helm repo update
	helm install certmgr jetstack/cert-manager \
        --set installCRDs=true \
        --version v1.8.0 \
        --namespace cert-manager \
        --create-namespace
	helm install featureform ./charts/featureform --set global.repo=local --set global.pullPolicy=Never --set global.version=stable
	kubectl get secret featureform-ca-secret -o=custom-columns=':.data.tls\.crt'| base64 -d > tls.crt
	export FEATUREFORM_HOST="localhost:443"
    export FEATUREFORM_CERT="tls.crt"

test_e2e: update_python					## Runs End-to-End tests on minikube
	pip3 install requests
	-helm install quickstart ./charts/quickstart
	kubectl wait --for=condition=complete job/featureform-quickstart-loader --timeout=360s
	kubectl wait --for=condition=READY=true pod -l app.kubernetes.io/name=ingress-nginx --timeout=360s
	kubectl wait --for=condition=READY=true pod -l app.kubernetes.io/name=etcd --timeout=360s
	kubectl wait --for=condition=READY=true pod -l chart=featureform --timeout=360s

	-kubectl port-forward svc/featureform-ingress-nginx-controller 8000:443 7000:80 &
	-kubectl port-forward svc/featureform-etcd 2379:2379 &

	while ! echo exit | nc localhost 7000; do sleep 10; done
	while ! echo exit | nc localhost 2379; do sleep 10; done

	featureform apply client/examples/quickstart.py --host localhost:8000 --cert tls.crt
	pytest client/tests/e2e.py

reset_e2e: etcd						## Resets Cluster. Requires install_etcd
	-kubectl port-forward svc/featureform-etcd 2379:2379 &
	while ! echo exit | nc localhost 2379; do sleep 10; done
	etcdctl --user=root:secretpassword del "" --prefix
	-helm uninstall quickstart


