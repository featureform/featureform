# This Source Code Form is subject to the terms of the Mozilla Public
# License, v.2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

import subprocess
import sys
import os
import shutil
import pytest
import random
import time

import embeddings as es
from embeddingstore import embedding_store_pb2

from data import _rand_int_emb

@pytest.fixture
def embedding_store_proc():
    proc = subprocess.Popen(os.path.join(os.environ["EMBEDDINGS_DIR"], 'bazel-out/darwin-fastbuild/bin/embeddingstore/main'))
    time.sleep(0.5)
    yield proc
    proc.kill()
    try:
        shutil.rmtree('embedding_store.dat')
    except Exception as err:
        print('cleanup error: {}'.format(err))
    try:
        shutil.rmtree('./.embeddings_cache/')
    except Exception as err:
        print('cleanup error: {}'.format(err))


@pytest.fixture
def es_client(embedding_store_proc):
    client = es.Client()
    yield client
    client.close()


@pytest.fixture
def store(es_client):
    es_client.create_store("users", 3)
    yield es_client.get_store("users")
    es_client.delete_store("users")


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
        "a": [10.1, 10.2, 10.3],
        "b": [10.1, 10.3, 10.3],
        "c": [9, 9, 9],
    }
    store.multiset(embs)
    neighbors = store.get_neighbors("a", 2)
    assert [n.key for n in neighbors] == ["b", "c"]
    

def test_training(es_client):
    store = es_client.create_store("users", 3)
    _embeddings = _rand_int_emb()
    store.multiset(_embeddings)
    key, _ = random.choice(list(_embeddings.items()))
    assert store.get(key) == _embeddings[key]
    store = es_client.get_training_store("users")
    store.download()
    assert store.get(key) == _embeddings[key]
    store.delete()


def test_training_upload(es_client):
    es_client.create_store("users", 3)
    _embeddings = _rand_int_emb(dimensions=3)
    # Test local read/writes
    store = es_client.get_training_store("users")
    store.multiset(_embeddings)
    key, _ = random.choice(list(_embeddings.items()))
    assert store.get(key) == _embeddings[key]
    store.upload()
    remote_store = es_client.get_store("users")
    assert remote_store.get(key) == _embeddings[key]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
