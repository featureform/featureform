# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

run_server: proto
	go run *.go

run_client: proto install_py
	python client.py

proto/serving_pb2.py: proto
	./gen_grpc.sh

install_py: requirements.txt
	pip install -r requirements.txt

