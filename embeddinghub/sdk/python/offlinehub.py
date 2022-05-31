# This Source Code Form is subject to the terms of the Mozilla Public
# License, v.2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
""" 
This module implements all base functionality of the EmbeddingStore in an offline setting.

It takes and EmbeddingHub space and downloads all of its data then indexs it. This is
useful when training for example.

Example:
    |  client = EmbeddingHubClient()
    |  dims = 3
    |  client.create_space("space", dims)
    |  client.set("space", "a", [1, 2, 3])
    |  offline = OfflineIndex(client.download("space"), dims)
    |  assert offline.get("space", "a") == [1, 2, 3]
"""

from collections import Mapping
import hnswlib


class Index:

    def __init__(self, key_emb_iter, dims):
        self._data = {}
        self._idx = hnswlib.Index("l2", dims)
        self._dims = dims
        self._mapper = HnswlibIndexMapper()
        self._size = 0
        self._cap = 1024
        self._idx.init_index(self._cap)
        self.multiset(key_emb_iter)

    def set(self, key, embedding):
        """Set key to embedding.

        Args:
            key: The embedding index to set.
            embedding: A python list representing the embedding vector to be
            stored.
        """
        if key not in self._data:
            self._size += 1
        self._data[key] = embedding
        idx = self._mapper.to_idx(key)
        self._idx.add_items([embedding], [idx])
        self._add_capacity_to_fit(1)

    def get(self, key):
        """Retrieves an embedding by key.
    
        Args:
            key: The embedding index for retrieval.

        Returns:
            An embedding, which is a python list of floats.
        """
        return self._data[key]

    def multiset(self, embedding_tuples):
        """Set multiple embeddings at once.

        Take a dictionary of key embedding pairs and set them in the index.

        Args:
            embedding_tuples: A dictionary from key to embedding or an iterator
            of key, embedding pairs where key is a string and embedding is a
            python list.
        """
        embeddings = []
        idxs = []
        if isinstance(embedding_tuples, Mapping):
            embedding_tuples = embedding_tuples.items()

        for key, embedding in embedding_tuples:
            embeddings.append(embedding)
            idxs.append(self._mapper.to_idx(key))
            if key not in self._data:
                self._size += 1
            self._data[key] = embedding
        # add_items fails if you give it two empty arrays.
        if len(idxs) == 0:
            return
        self._add_capacity_to_fit(len(idxs))
        self._idx.add_items(embeddings, idxs)

    def multiget(self, keys):
        """Get multiple embeddings at once.

        Args:
            keys: An iterator of embedding indices for retrieval.

        Returns:
            A list of embeddings.
        """
        return [self._data[key] for key in keys]

    def nearest_neighbor(self, num, key=None, embedding=None):
        """Finds N nearest neighbors for a given embedding record.

        Args:
            key: The embedding index to get the nearest neighbors of.
            num: The number of nearest neighbors.

        Returns:
            A num size list of embedding vectors that are closest to the
            provided embedding.
        """
        has_key = key is not None
        if has_key:
            embedding = self._data[key]
            # We have to remove the key from the results later
            num_retrieve = num + 1
        else:
            num_retrieve = num
        list_results, list_distances = self._idx.knn_query(
            embedding, num_retrieve)
        results = list_results[0]
        if has_key:
            idx = self._mapper.to_idx(key)
            results = [
                self._mapper.to_key(result)
                for result in results
                if result != idx
            ]
            # If the key wasn't found in the results, we still should return
            # only num results.
            if len(results) > num:
                results = results[:-1]
        return results

    def size(self):
        """Returns the amount of embeddings in the index"""
        return self._size

    def _add_capacity_to_fit(self, size):
        min_cap = self._size + size
        if min_cap > self._cap:
            self._cap *= 2
            self._idx.resize_index(self._cap)


class HnswlibIndexMapper:
    """ Maps keys to atomically increasing ints

    hnswlib.Index expects labels to be atomically increasing ints, but our API
    handles any type of key. This class is used for mapping back and forth
    """

    def __init__(self):
        self._key_to_idx = {}
        self._idx_to_key = {}
        self._next_idx = 0

    def to_idx(self, key):
        """ Turn a key into an hnswlib index

        Args:
            key: The key to transform

        Returns:
            An index to be used in hnswlib
        """
        if key in self._key_to_idx:
            return self._key_to_idx[key]
        else:
            idx = self._next_idx
            self._next_idx += 1
            self._key_to_idx[key] = idx
            self._idx_to_key[idx] = key
            return idx

    def to_key(self, idx):
        """ Turn a hnswlib index into the original key

        Args:
            idx: an hnswlib index

        Returns:
            The key associated with the index
        """
        return self._idx_to_key[idx]
