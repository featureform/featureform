# This Source Code Form is subject to the terms of the Mozilla Public
# License, v.2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

import subprocess
import sys
import time
import os
import pytest

import client.embeddingstore as es


@pytest.fixture
def embedding_store_proc():
    proc = subprocess.Popen(os.environ["TEST_SRCDIR"] +
                            "/__main__/embeddingstore/main")
    time.sleep(1)
    yield proc
    proc.kill()


@pytest.fixture
def es_client(embedding_store_proc):
    client = es.EmbeddingStoreClient()
    yield client
    client.close()


def test_set_get(es_client):
    space = "test"
    emb = [1, 2, 3]
    es_client.set(space, "a", emb)
    assert es_client.get(space, "a") == emb


def test_multiset_get(es_client):
    space = "test"
    embs = {
        "a": [1, 2, 3],
        "b": [3, 2, 1],
    }
    es_client.multiset(space, embs)
    for key, emb in embs.items():
        assert es_client.get(space, key) == emb

def test_multiset_multiget(es_client):
    embs = {
        "a": [1, 2, 3],
        "b": [3, 2, 1],
    }
    es_client.multiset(embs)
    resp_embs = es_client.multiget(embs.keys())
    resp_emb_dict = {key: val for key, val in zip(embs.keys(), resp_embs)}
    assert embs == resp_emb_dict


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
