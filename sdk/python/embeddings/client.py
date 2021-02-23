# This Source Code Form is subject to the terms of the Mozilla Public
# License, v.2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

import grpc

from embeddingstore import embedding_store_pb2, embedding_store_pb2_grpc

class Store(object):

    def __init__(self, stub, name):
        self.name = name
        self._stub = stub

    def get(self, key):
        resp = self._stub.Get(embedding_store_pb2.GetRequest(store_name=self.name, key=key))
        return resp.embedding.values

    def set(self, key, embedding):
        req = embedding_store_pb2.SetRequest()
        req.store_name = self.name
        req.key = key
        req.embedding.values[:] = embedding
        self._stub.Set(req)

    def multiset(self,  embedding_dict):
        it = self._embedding_dict_iter(embedding_dict)
        self._stub.MultiSet(it)

    def _embedding_dict_iter(self, embedding_dict):
        for key, embedding in embedding_dict.items():
            req = embedding_store_pb2.MultiSetRequest()
            req.store_name = self.name
            req.key = key
            req.embedding.values[:] = embedding
            yield req

    def get_neighbors(self, key, number):
        req = embedding_store_pb2.GetNeighborsRequest()
        req.store_name = self.name
        req.key = key
        req.number = number
        out = []
        for n in self._stub.GetNeighbors(req):
            out.append(n)
        return out

class Client:

    def __init__(self, host="localhost", port=50051):
        connection_str = "%.%".format(host, port)
        self._channel = grpc.insecure_channel('localhost:50051')
        self._stub = embedding_store_pb2_grpc.EmbeddingStoreStub(self._channel)

    def close(self):
        return self._channel.close()

    def create_store(self, name, dimensions):
        req = embedding_store_pb2.CreateStoreRequest()
        req.store_name = name
        req.dimensions = dimensions
        self._stub.CreateStore(req)
        return Store(self._stub, name)

    def delete_store(self, name):
        req = embedding_store_pb2.DeleteStoreRequest()
        req.store_name = name
        self._stub.DeleteStore(req)

    def get_store(self, name):
        return Store(self._stub, name)

    def set(self, store, key, embedding):
        req = embedding_store_pb2.SetRequest()
        req.store_name = store
        req.key = key
        req.embedding.values[:] = embedding
        self._stub.Set(req)

    def get(self, store, key):
        resp = self._stub.Get(embedding_store_pb2.GetRequest(store_name=store, key=key))
        return resp.embedding.values

    def multiset(self, store, embedding_dict):
        it = self._embedding_dict_iter(store, embedding_dict)
        self._stub.MultiSet(it)

    def _embedding_dict_iter(self, store, embedding_dict):
        for key, embedding in embedding_dict.items():
            req = embedding_store_pb2.MultiSetRequest()
            req.store_name = store
            req.key = key
            req.embedding.values[:] = embedding
            yield req

    def get_neighbors(self, store, key, number):
        req = embedding_store_pb2.GetNeighborsRequest()
        req.store_name = store
        req.key = key
        req.number = number
        out = []
        for n in self._stub.GetNeighbors(req):
            out.append(n)
        return out
