# This Source Code Form is subject to the terms of the Mozilla Public
# License, v.2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

import subprocess
import sys
import os
import pytest
import time

import embeddings as es


@pytest.fixture
def embedding_store_proc():
    proc = subprocess.Popen(os.path.join(os.environ["EMBEDDINGS_DIR"], 'bazel-out/darwin-fastbuild/bin/embeddingstore/main'))
    time.sleep(0.5)
    yield proc
    proc.kill()

@pytest.fixture
def es_client(embedding_store_proc):
    client = es.Client()
    yield client
    client.close()

@pytest.fixture
def store(es_client):
    es_client.create_store("users", 3)
    yield es_client.get_store("users")


def test_set_get(store):

    emb = [1, 2, 3]
    store.set("a", emb)
    assert store.get("a") == emb


def test_multiset_get(store):
    embs = {
        "a": [1, 2, 3],
        "b": [3, 2, 1],
    }
    store.multiset(embs)
    for key, emb in embs.items():
        assert store.get(key) == emb


def test_nn(store):
    embs = {
        "test_nn.a": [10.1, 10.2, 10.3],
        "test_nn.b": [10.1, 10.3, 10.3],
        "test_nn.c": [9, 9, 9],
    }
    store.multiset(embs)
    neighbors = store.get_neighbors("test_nn.a", 2)
    print('neighbors: {}'.format(neighbors))
    assert [n.key for n in neighbors] == ["test_nn.b", "test_nn.c"]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
