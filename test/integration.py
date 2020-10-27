# This Source Code Form is subject to the terms of the Mozilla Public
# License, v.2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

import subprocess
import sys
import os
import pytest

import client.client as es


@pytest.fixture
def embedding_store_proc():
    proc = subprocess.Popen(os.environ["TEST_SRCDIR"] +
                            "/__main__/embeddingstore/main")
    yield proc
    proc.kill()


@pytest.fixture
def es_client(embedding_store_proc):
    client = es.EmbeddingStoreClient()
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


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
