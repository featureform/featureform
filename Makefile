# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

##############################################  HELP  ##################################################################

define HELP_BODY
To run unit tests, run:
	make test

usage: make [target] [options]
TARGETS
help
	Description:
		Prints this help message

init
	Requirements:
		- Python 3.7-3.10
		- Golang 1.18

	Description:
		Installs grpc-tools with pip and builds the proto files for the serving and metadata connections for the
		Python client and Golang libraries. It then builds and installs the Python SDK and CLI.


test
	Requirements:
		- Python 3.7-3.10
		- Golang 1.18
	Description:
		Runs 'init' then runs the Python and Golang Unit tests

test_offline
	Requirements:
		- Golang 1.18

	Description:
		Runs offline store integration tests. Requires credentials if not using the memory provider

	Options:
		- provider (memory | postgres | snowflake | redshift | bigquery | spark )
			Description:
				Runs specified provider. If left blank or not included, runs all providers
			Usage:
				make test_offline provider=memory

test_online
	Requirements:
		- Golang 1.18

	Description:
		Runs online store integration tests. Requires credentials if not using the memory or redis_mock provider

	Options:
		- provider (memory | redis_mock | redis_insecure | redis_secure | cassandra | firestore | dynamo )
			Description:
				Runs specified provider. If left blank or not included, runs all providers
			Usage:
				make test_online provider=memory


endef
export HELP_BODY

help:
	@echo "$$HELP_BODY"  |  less

##############################################  UNIT TESTS #############################################################

init: update_python

test: init pytest test_go_unit

##############################################  SETUP ##################################################################

gen_grpc:						## Generates GRPC Dependencies
	python3 -m pip install grpcio-tools

	-mkdir client/src/featureform/proto/
	cp metadata/proto/metadata.proto client/src/featureform/proto/metadata.proto
	cp proto/serving.proto client/src/featureform/proto/serving.proto

	protoc --go_out=. --go_opt=paths=source_relative     --go-grpc_out=. --go-grpc_opt=paths=source_relative     ./proto/serving.proto
	python3 -m grpc_tools.protoc -I ./client/src --python_out=./client/src --grpc_python_out=./client/src/ ./client/src/featureform/proto/serving.proto

	protoc --go_out=. --go_opt=paths=source_relative     --go-grpc_out=. --go-grpc_opt=paths=source_relative     ./metadata/proto/metadata.proto
	python3 -m grpc_tools.protoc -I ./client/src --python_out=./client/src/ --grpc_python_out=./client/src/ ./client/src/featureform/proto/metadata.proto

update_python: gen_grpc 				## Updates the python package locally
	pip3 install pytest
	pip3 install build
	pip3 uninstall featureform  -y
	-rm -r client/dist/*
	python3 -m build ./client/
	pip3 install client/dist/*.whl
	pip3 install -r provider/scripts/requirements.txt

etcdctl: 						## Installs ETCDCTL. Required for reset_e2e
	-git clone -b v3.4.16 https://github.com/etcd-io/etcd.git
	cd etcd && ./build
	export PATH=$PATH:"`pwd`/etcd/bin"
	etcdctl version

credentials:
	-mkdir ~/credentials
	aws secretsmanager get-secret-value --secret-id bigquery.json --region us-east-1 |   jq -r '.SecretString' > ~/credentials/bigquery.json
	aws secretsmanager get-secret-value --secret-id firebase.json --region us-east-1 |   jq -r '.SecretString' > ~/credentials/firebase.json
	aws secretsmanager get-secret-value --secret-id .env --region us-east-1 |   jq -r '.SecretString' |   jq -r "to_entries|map(\"\(.key)=\\\"\(.value|tostring)\\\"\")|.[]" > .env

start_postgres:
	-docker kill postgres
	-docker rm postgres
	docker run -d -p 5432:5432 --name postgres -e POSTGRES_PASSWORD=password postgres

stop_postgres:
	docker kill postgres
	docker rm postgres

##############################################  PYTHON TESTS ###########################################################
pytest:
	-rm -r .featureform
	curl -C - https://featureform-demo-files.s3.amazonaws.com/transactions.csv -o transactions.csv
	pytest client/tests/serving_test.py
	pytest client/src/featureform/local_dash_test.py
	pytest client/tests/redefined_test.py
	pytest client/tests/local_test.py
	pytest client/tests/localmode_quickstart_test.py
	pytest client/tests/register_test.py
	pip3 install jupyter nbconvert matplotlib pandas scikit-learn requests
	jupyter nbconvert --to notebook --execute notebooks/Fraud_Detection_Example.ipynb
	-rm -r .featureform

test_pyspark:
	pytest -v --cov=offline_store_spark_runner provider/scripts/tests/ --cov-report term-missing
##############################################  GO TESTS ###############################################################

test_offline: gen_grpc 					## Run offline tests. Run with `make test_offline provider=(memory | postgres | snowflake | redshift | spark )`
	@echo "These tests require a .env file. Please Check .env-template for possible variables"
	-mkdir coverage
	go test -v -timeout 60m -coverpkg=./... -coverprofile coverage/cover.out.tmp ./provider/... --tags=offline,spark --provider=$(provider)

test_online: gen_grpc 					## Run offline tests. Run with `make test_online provider=(memory | redis_mock | redis_insecure | redis_secure | cassandra | firestore | dynamo )`
	@echo "These tests require a .env file. Please Check .env-template for possible variables"
	-mkdir coverage
	go test -v -coverpkg=./... -coverprofile coverage/cover.out.tmp ./provider/... --tags=online,provider --provider=$(provider)

test_go_unit:
	-mkdir coverage
	go test ./... -tags=*,offline,provider --short   -coverprofile coverage/cover.out.tmp

test_metadata:							## Requires ETCD to be installed and added to path
	-mkdir coverage
	$(flags) etcd &
	sleep 1
	go test -coverpkg=./... -coverprofile coverage/cover.out.tmp ./metadata/

test_helpers:
	-mkdir coverage
	go test -v -coverpkg=./... -coverprofile coverage/cover.out.tmp ./helpers/...

test_serving:
	-mkdir coverage
	go test -v -coverpkg=./... -coverprofile coverage/cover.out.tmp ./newserving/...

test_runner:							## Requires ETCD to be installed and added to path
	-mkdir coverage
	$(flags) etcd &
	sleep 1
	go test -v -coverpkg=./... -coverprofile coverage/cover.out.tmp ./runner/...




##############################################  MINIKUBE ###############################################################

containers: gen_grpc						## Build Docker containers for Minikube
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
	kubectl wait --for=condition=complete job/featureform-quickstart-loader --timeout=720s
	kubectl wait --for=condition=READY=true pod -l app.kubernetes.io/name=ingress-nginx --timeout=720s
	kubectl wait --for=condition=READY=true pod -l app.kubernetes.io/name=etcd --timeout=720s
	kubectl wait --for=condition=READY=true pod -l chart=featureform --timeout=720s

	-kubectl port-forward svc/featureform-ingress-nginx-controller 8000:443 7000:80 &
	-kubectl port-forward svc/featureform-etcd 2379:2379 &

	while ! echo exit | nc localhost 7000; do sleep 10; done
	while ! echo exit | nc localhost 2379; do sleep 10; done

	featureform apply client/examples/quickstart.py --host localhost:8000 --cert tls.crt
	pytest client/tests/e2e.py

reset_e2e:  			 			## Resets Cluster. Requires install_etcd
	-kubectl port-forward svc/featureform-etcd 2379:2379 &
	while ! echo exit | nc localhost 2379; do sleep 10; done
	etcdctl --user=root:secretpassword del "" --prefix
	-helm uninstall quickstart
