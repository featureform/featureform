# This Source Code Form is subject to the terms of the Mozilla Public
# License, v.2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

""" 
This module provides a client to talk to the embedding store server. 

It contains methods for setting and retrieving embeddings as well as nearest neighbor search.

Example:
    |  client = EmbeddingStoreClient()
    |  client.set("a", [1, 2, 3])
    |  assert client.get("a") == [1, 2, 3]
"""

import grpc
from client import embedding_store_pb2_grpc
from client import embedding_store_pb2


class EmbeddingStoreClient:

    def __init__(self, host="localhost", port=50051):
        connection_str = "%.%".format(host, port)
        self._channel = grpc.insecure_channel('localhost:50051')
        self._stub = embedding_store_pb2_grpc.EmbeddingStoreStub(self._channel)

    def close(self):
        """Closes the connection.
        """
        return self._channel.close()

    def set(self, space, key, embedding):
        """Set key to embedding on the server.

        Sets an embedding record with a key.
        The vector representation is stored as a dictionary.
        example: {'key': []}

        Args:
            space: The name of the space to write to.
            key: The embedding index for retrieval.
            embedding: A python list of the embedding vector to be stored. 
        """
        req = embedding_store_pb2.SetRequest()
        req.space = space
        req.key = key
        req.embedding.values[:] = embedding
        self._stub.Set(req)

    def get(self, space, key):
        """Retrieves an embedding record from the server.
    
        Args: 
            space: The name of the space to write to.
            key: The embedding index for retrieval.

        Returns:
            A dictionary from key to embedding, where key is a string and 
            embedding is a python list.        
        """
        resp = self._stub.Get(embedding_store_pb2.GetRequest(space=space, key=key))
        return resp.embedding.values

    def multiset(self, space, embedding_dict):
        """Set multiple embeddings at once.

        Take a dictionary of key embedding pairs and set them on the server.
        example: {'key': embedding_pairs}

        Args: 
            space: The name of the space to write to.
            embedding_dict: A dictionary from key to embedding, where key is a string and 
            embedding is a python list.
        """
        it = self._embedding_dict_iter(space, embedding_dict)
        self._stub.MultiSet(it)
    
    def multiget(self, keys):
        it = self._key_iter(keys)
        return self._embedding_iter(self._stub.MultiGet(it))


    def nearest_neighbor(self, space, key, num):
        """Finds N nearest neighbors for a given embedding record.

        Args: 
            space: The name of the space to retrieve from.
            key: The embedding index for retrieval.
            num: The number of nearest neighbors.

        Returns:
            A num size list of embedding vectors that are closest to the provided vector embedding.
        """
        req = embedding_store_pb2.NearestNeighborRequest(space=space, key=key, num=num)
        return self._stub.NearestNeighbor(req).keys

    def _embedding_dict_iter(self, space, embedding_dict):
        """Create an iterator from an embedding dict.

        Args: 
            embedding_dict: A dictionary from key to embedding, where key is a string and 
            embedding is a python list.
        """
        for key, embedding in embedding_dict.items():
            req = embedding_store_pb2.MultiSetRequest()
            req.space = space
            req.key = key
            req.embedding.values[:] = embedding
            yield req

    def _key_iter(self, keys):
        for key in keys:
            req = embedding_store_pb2.MultiGetRequest()
            req.key = key
            yield req

    def _embedding_iter(self, resps):
        for resp in resps:
            yield resp.embedding.values

