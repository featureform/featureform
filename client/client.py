# This Source Code Form is subject to the terms of the Mozilla Public
# License, v.2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

import grpc

from embeddingstore import embedding_store_pb2, embedding_store_pb2_grpc


class EmbeddingStoreClient:

    def __init__(self, host="localhost", port=50051):
        connection_str = "%.%".format(host, port)
        self._channel = grpc.insecure_channel('localhost:50051')
        self._stub = embedding_store_pb2_grpc.EmbeddingStoreStub(self._channel)

    def close(self):
        return self._channel.close()

    def set(self, key, embedding):
        req = embedding_store_pb2.SetRequest()
        req.key = key
        req.embedding.values[:] = embedding
        self._stub.Set(req)

    def get(self, key):
        resp = self._stub.Get(embedding_store_pb2.GetRequest(key=key))
        return resp.embedding.values
