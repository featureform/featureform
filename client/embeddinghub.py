# This Source Code Form is subject to the terms of the Mozilla Public
# License, v.2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
""" 
This module provides a client to talk to the embedding hub server. 

It contains methods for setting and retrieving embeddings as well as nearest neighbor search.

Example:
    |  client = EmbeddingHubClient()
    |  client.set("a", [1, 2, 3])
    |  assert client.get("a") == [1, 2, 3]
"""

import grpc
from client import embedding_store_pb2_grpc
from client import embedding_store_pb2


class EmbeddingHubClient:

    def grpc_channel(host="0.0.0.0", port=50051):
        connection_str = "{}:{}".format(host, port)
        return grpc.insecure_channel(connection_str,
                                     options=(('grpc.enable_http_proxy', 0),))

    def __init__(self, grpc_channel=None, host="0.0.0.0", port=50051):
        if grpc_channel is not None:
            self._channel = grpc_channel
        else:
            self._channel = EmbeddingHubClient.grpc_channel(host, port)
        self._stub = embedding_store_pb2_grpc.EmbeddingHubStub(self._channel)

    def close(self):
        """Closes the connection.
        """
        return self._channel.close()

    def create_space(self, name, dims):
        """Create a new space in the embedding hub.

        A space is essentially a table. It can contain multiple different
        version and also be immutable. This method creates a new space with
        the given number of dimensions.

        Args:
            name: The name of the space to create.
            dims: The number of dimensions that an embedding in the space will
            have.
        """
        req = embedding_store_pb2.CreateSpaceRequest()
        req.name = str(name)
        req.dims = dims
        self._stub.CreateSpace(req)

    def freeze_space(self, name):
        """Make an existing space immutable.

        After this call, the space cannot be updated. This call cannot be
        reversed.

        Args:
            name: The name of the space to freeze.
        """
        req = embedding_store_pb2.FreezeSpaceRequest()
        req.name = str(name)
        self._stub.FreezeSpace(req)

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
        req.space = str(space)
        req.key = str(key)
        req.embedding.values[:] = embedding
        try:
            self._stub.Set(req)
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.FAILED_PRECONDITION:
                raise TypeError(e.details())
            raise

    def get(self, space, key):
        """Retrieves an embedding record from the server.
    
        Args:
            space: The name of the space to write to.
            key: The embedding index for retrieval.

        Returns:
            An embedding, which is a python list of floats.
        """
        resp = self._stub.Get(
            embedding_store_pb2.GetRequest(space=str(space), key=str(key)))
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

    def multiget(self, space, keys):
        """Get multiple embeddings at once.

        Get multiple embeddings by key in the same space.

        Args:
            space: The name of the space to get from.
            keys: A list of embedding indices for retrieval.

        Returns:
            A list of embeddings.
        """
        it = self._key_iter(space, keys)
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
        req = embedding_store_pb2.NearestNeighborRequest(space=str(space),
                                                         key=str(key),
                                                         num=num)
        return self._stub.NearestNeighbor(req).keys

    def _embedding_dict_iter(self, space, embedding_dict):
        """Create a MultiSetRequest iterator from a space and an embedding dict.

        Args:
            space: The name of the space to set in the MultiSetRequests.
            embedding_dict: A dictionary from key to embedding, where key is a string and 
            embedding is a python list. These will be transformed into MutliSetRequests.

        Returns:
            An iterator of MultiSetRequest.
        """
        for key, embedding in embedding_dict.items():
            req = embedding_store_pb2.MultiSetRequest()
            req.space = str(space)
            req.key = str(key)
            req.embedding.values[:] = embedding
            yield req

    def _key_iter(self, space, keys):
        """Create an MultiGetRequest iterator from a list of keys and a space.

        Args:
            space: The name of the space to set in the MultiGetRequests.
            keys: A list of keys to turn into MultiGetRequests.

        Returns:
            An iterator of MultiGetRequest.
        """
        for key in keys:
            req = embedding_store_pb2.MultiGetRequest()
            req.space = str(space)
            req.key = str(key)
            yield req

    def _embedding_iter(self, resps):
        """Unwrap an iterator of MultiGetResponse

        Args:
            resps: An iterator of MultiGetResponse

        Returns:
            An iterator of embeddings.
        """
        for resp in resps:
            yield resp.embedding.values
