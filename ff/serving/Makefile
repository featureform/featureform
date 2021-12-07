run_server: proto/serving_pb2.py
	go run serving.go

run_client: proto/serving_pb2.py install_py
	python client.py

proto/serving_pb2.py: proto/serving.proto
	./gen_grpc.sh

install_py: requirements.txt
    pip install -r requirements.txt
