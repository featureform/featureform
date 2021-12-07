import grpc
import proto.serving_pb2
import proto.serving_pb2_grpc

channel = grpc.insecure_channel('localhost:8080',
                                options=(('grpc.enable_http_proxy', 0),))
stub = proto.serving_pb2_grpc.OfflineServingStub(channel)

req = proto.serving_pb2.TrainingDataRequest()
req.id.name = "abc"

print([x for x in stub.TrainingData(req)])
