import subprocess
import sys
import os
import pytest
import time

import embeddings as es


@pytest.fixture
def embedding_store_proc():
    proc = subprocess.Popen(os.path.join(os.environ["EMBEDDINGS_DIR"], 'bazel-out/darwin-fastbuild/bin/embeddingstore/main'))
    time.sleep(0.3)
    yield proc
    proc.kill()


@pytest.fixture
def es_client(embedding_store_proc):
    client = es.Client()
    yield client
    client.close()


def test_set_get(es_client):
    emb = [1, 2, 3]
    es_client.set("a", emb)
    assert es_client.get("a") == emb


def test_multiset_get(es_client):
    embs = {
        "a": [1, 2, 3],
        "b": [3, 2, 1],
    }
    es_client.multiset(embs)
    for key, emb in embs.items():
        assert es_client.get(key) == emb


def test_nn(es_client):
    embs = {
        "a": [1.1, 1.2, 1.3],
        "b": [1.1, 1.3, 1.3],
        "c": [0, 0, 0],
    }
    es_client.multiset(embs)
    neighbors = es_client.get_neighbors("a", 2)
    print('neighbors: {}'.format(neighbors))
    assert [n.key for n in neighbors] == ["b", "c"]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
