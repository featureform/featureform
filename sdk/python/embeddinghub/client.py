# This Source Code Form is subject to the terms of the Mozilla Public
# License, v.2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

import concurrent.futures
import grpc
from collections.abc import Mapping
from embeddinghub import offlinehub
from embeddinghub import embedding_store_pb2_grpc
from embeddinghub import embedding_store_pb2


def connect(config):
    if type(config) is LocalConfig:
        return LocalEmbeddingHub(config)
    elif type(config) is Config:
        return EmbeddingHub(config)
    else:
        raise Exception("Invalid Config")


class EmbeddingHub:

    def __init__(self, config):
        self._client = EmbeddingHubClient(host=config.host, port=config.port)

    def get_space(self, name, download_snapshot=False):
        self._client.check_space(name)
        space = Space(self._client, name)
        if download_snapshot:
            return space.download_snapshot()
        return space

    def create_space(self, name, dims):
        self._client.create_space(name, dims)
        return Space(self._client, name)

    def delete_space(self, name):
        self._client.delete_space(name)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def close(self):
        self._client.close()


class LocalEmbeddingHub:

    def __init__(self, config):
        self.spaces = {}

    def get_space(self, name, download_snapshot=False):
        space = Space(self._client, name)
        if download_snapshot:
            return space.download_snapshot()
        return space

    def create_space(self, name, dims):
        if name in self.spaces:
            raise Exception("Space already exists")
        space = SpaceSnapshot([], dims)
        self.spaces[name] = space
        return space

    def delete_space(self, name):
        del self.spaces[name]

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.save()

    def save(self):
        print("SAVE NOT IMPLEMENTED YET")


class SpaceSnapshot:

    def __init__(self, keyed_embeddings, dims=None):
        self._idx = offlinehub.Index(keyed_embeddings, dims=dims)

    def set(self, key, embedding):
        self._idx.set(key, embedding)

    def get(self, key):
        self._idx.get(key)

    def multiset(self, keyed_embeddings):
        self._idx.multiset(keyed_embeddings)

    def multiget(self, keys):
        return self._idx.multiget(keys)

    def iterator(self):
        return self._idx.iterator()

    def nearest_neighbors(self, num, key=None, vector=None):
        return self._idx.nearest_neighbor(num, key=key, embedding=vector)

    def multi_nearest_neighbors(self, num, keys=None, vectors=None):
        raise NotImplementedError()

    def write_to_space(self, name, create=False):
        if create:
            self._client.create_space(name, dims=self.dims)
        self._client.multiset(name, self.iterator())

    def overwrite_space(self, name):
        raise NotImplementedError()

    def refresh(self):
        if client is None:
            return
        self.multiset(self._client.download(self._name))


class Space:

    def __init__(self, client, name, dims=None):
        self._client = client
        self._name = name
        self._dims = dims

    def download_snapshot(self):
        return SpaceSnapshot(self._client.download(self._name), dims=dims)

    def delete(self, key):
        return self._client.delete(self._name, key)

    def multidelete(self, keys):
        return self._client.multidelete(self._name, keys)

    def delete_all(self):
        raise NotImplementedError()

    def set(self, key, embedding):
        self._client.set(self._name, key, embedding)

    def get(self, key):
        return self._client.get(self._name, key)

    def multiset(self, keyed_embeddings):
        return self._client.multiset(self._name, keyed_embeddings)

    def multiget(self, keys):
        return self._client.multiget(self._name, keys)

    def iterator(self):
        return self._client.download(self._name)

    def nearest_neighbors(self, num, key=None, vector=None):
        return self._client.nearest_neighbor(self._name,
                                             num,
                                             key=key,
                                             embedding=vector)

    def multi_nearest_neighbor(self, num, keys=None, vectors=None):
        raise NotImplementedError()


class Config:

    def __init__(self, host="0.0.0.0", port="7462"):
        self.host = host
        self.port = port


class LocalConfig:

    def __init__(self, directory):
        self.directory = directory


class EmbeddingHubClient:

    def grpc_channel(host="0.0.0.0", port=7462):
        connection_str = "{}:{}".format(host, port)
        return grpc.insecure_channel(connection_str,
                                     options=(('grpc.enable_http_proxy', 0),))

    def __init__(self, host="0.0.0.0", port=7462, grpc_channel=None):
        if grpc_channel is not None:
            self._channel = grpc_channel
        else:
            self._channel = EmbeddingHubClient.grpc_channel(host, port)
        self._stub = embedding_store_pb2_grpc.EmbeddingHubStub(self._channel)

    def close(self):
        """Closes the connection.
        """
        return self._channel.close()

    def create_space(self, name, dims, wait=True):
        """Create a new space in the embedding hub.

        A space is essentially a table. It can contain multiple different
        version and also be immutable. This method creates a new space with
        the given number of dimensions.

        Args:
            name: The name of the space to create.
            dims: The number of dimensions that an embedding in the space will
            have.
            wait: A bool which specifies if the call should be synchronous.

        Returns:
            A future if wait is False.
        """
        req = embedding_store_pb2.CreateSpaceRequest()
        req.name = str(name)
        req.dims = dims
        future = self._stub.CreateSpace.future(req)
        if wait:
            return future.result()
        return future

    def check_space(self, name, wait=True):
        req = embedding_store_pb2.CheckSpaceRequest()
        req.name = str(name)
        future = self._stub.CheckSpace.future(req)
        if wait:
            return future.result()
        return future

    def delete_space(self, name, wait=True):
        req = embedding_store_pb2.DeleteSpaceRequest()
        req.name = str(name)
        future = self._stub.DeleteSpace.future(req)
        if wait:
            return future.result()
        return future

    def freeze_space(self, name, wait=True):
        """Make an existing space immutable.

        After this call, the space cannot be updated. This call cannot be
        reversed.

        Args:
            name: The name of the space to freeze.
            wait: A bool which specifies if the call should be synchronous.

        Returns:
            A future if wait is False.
        """
        req = embedding_store_pb2.FreezeSpaceRequest()
        req.name = str(name)
        future = self._stub.FreezeSpace.future(req)
        if wait:
            return future.result()
        return future

    def set(self, space, key, embedding, wait=True):
        """Set key to embedding on the server.

        Sets an embedding record with a key.
        The vector representation is stored as a dictionary.
        example: {'key': []}

        Args:
            space: The name of the space to write to.
            key: The embedding index for retrieval.
            embedding: A python list of the embedding vector to be stored. 
            wait: A bool which specifies if the call should be synchronous.

        Returns:
            A future if wait is False.
        """
        req = embedding_store_pb2.SetRequest()
        req.space = str(space)
        req.key = str(key)
        req.embedding.values[:] = embedding
        future = self._stub.Set.future(req)
        if wait:
            try:
                future.result()
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.FAILED_PRECONDITION:
                    raise TypeError(e.details())
                raise
        return future

    def delete(self, space, key, wait=True):
        req = embedding_store_pb2.DeleteRequest()
        req.space = str(space)
        req.key = str(key)
        future = self._stub.Set.future(req)
        if wait:
            try:
                future.result()
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.FAILED_PRECONDITION:
                    raise TypeError(e.details())
                raise
        return future

    def get(self, space, key, wait=True):
        """Retrieves an embedding record from the server.
    
        Args:
            space: The name of the space to write to.
            key: The embedding index for retrieval.
            wait: A bool which specifies if the call should be synchronous.

        Returns:
            An embedding, which is a python list of floats. If wait is False,
            the value will be wrapped in a future.
        """
        req = embedding_store_pb2.GetRequest(space=str(space), key=str(key))
        future = self._stub.Get.future(req)
        transform_fn = lambda res: res.embedding.values
        wrapped_future = FutureTransformWrapper(future, transform_fn)
        if wait:
            return wrapped_future.result()
        return wrapped_future

    def multiset(self, space, embedding_tuples):
        """Set multiple embeddings at once.

        Take a dictionary of key embedding pairs and set them on the server.
        example: {'key': embedding_pairs}

        Args:
            space: The name of the space to write to.
            embedding_tuples: A dictionary from key to embedding or an iterator
            of key, embedding pairs where key is a string and embedding is a
            python list.
        """
        if isinstance(embedding_tuples, Mapping):
            embedding_tuples = embedding_tuples.items()
        it = self._embedding_tuples_iter(space, embedding_tuples)
        self._stub.MultiSet(it)

    def multiget(self, space, keys):
        """Get multiple embeddings at once.

        Get multiple embeddings by key in the same space.

        Args:
            space: The name of the space to get from.
            keys: An iterator of embedding indices for retrieval.

        Returns:
            A list of embeddings.
        """
        it = self._key_iter(space, keys)
        return self._embedding_iter(self._stub.MultiGet(it))

    def multidelete(self, space, keys):
        it = self._delete_key_iter(space, keys)
        self._stub.MultiDelete(it)

    def nearest_neighbor(self, space, num, key=None, embedding=None, wait=True):
        """Finds N nearest neighbors for a given embedding record.

        Args:
            space: The name of the space to retrieve from.
            key: The embedding index for retrieval.
            num: The number of nearest neighbors.
            wait: A bool which specifies if the call should be synchronous.

        Returns:
            A num size list of embedding vectors that are closest to the
            provided vector embedding. If wait is False, the value will
            be wrapped in a future.
        """
        if (key is not None):
            key = str(key)
        req = embedding_store_pb2.NearestNeighborRequest(space=str(space),
                                                         key=key,
                                                         embedding=embedding,
                                                         num=num)
        future = self._stub.NearestNeighbor.future(req)
        transform_fn = lambda res: res.keys
        wrapped_future = FutureTransformWrapper(future, transform_fn)
        if wait:
            return wrapped_future.result()
        return wrapped_future

    def download(self, space):
        """Get all values in the space provided.

        Args:
            space: The name of the space to retrieve from.

        Returns:
            An iterator of key-embedding pairs.
        """
        req = embedding_store_pb2.DownloadRequest(space=str(space))
        return self._download_iter(self._stub.Download(req))

    def _embedding_tuples_iter(self, space, it):
        """Create a MultiSetRequest iterator from a space and an iterator of
        key-embedding tuples.

        Args:
            space: The name of the space to set in the MultiSetRequests.
            it: An iterator of key, embedding tuples, where key is a string and
            embedding is a python list. These will be transformed into
            MultiSetRequests.

        Returns:
            An iterator of MultiSetRequest.
        """
        for key, embedding in it:
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

    def _delete_key_iter(self, space, keys):
        for key in keys:
            req = embedding_store_pb2.MultiDeleteRequest()
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

    def _download_iter(self, resps):
        """Unwrap an iterator of DownloadResponse

        Args:
            resps: An iterator of DownloadResponse

        Returns:
            An iterator of key, embedding pairs.
        """
        for resp in resps:
            yield (resp.key, resp.embedding.values)


class FutureTransformWrapper:
    """A wrapper around a future that runs transform_fn on the result.

    Some libraries like gRPC return futures. This class allows us to return a
    future that does a simple transformation on the result, and is equivalent
    to the wrapped future in all other ways.
    """

    def __init__(self, future, transform_fn):
        self._future = future
        self._transform = transform_fn

    def __getattr__(self, attr):
        """Pass all other attribute access to the inner _future object.
        """
        return getattr(self._future, attr)

    def result(self, timeout=None):
        raw_result = self._future.result(timeout)
        return self._transform(raw_result)

    def add_done_callback(self, fn):

        def wrapped_callback_fn(fut):
            unwrapped_val = self._transform(fut.result())
            inner_fut = concurrent.futures.Future()
            inner_fut.set_result(unwrapped_val)
            fn(inner_fut)

        self._future.add_done_callback(wrapped_callback_fn)
