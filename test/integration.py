# This Source Code Form is subject to the terms of the Mozilla Public
# License, v.2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

import subprocess
import sys
import time
import os
import pytest
import uuid

import client.embeddinghub as es


@pytest.fixture
def embedding_hub_proc():
    proc = subprocess.Popen(os.environ["TEST_SRCDIR"] +
                            "/__main__/embeddingstore/main")
    time.sleep(1)
    yield proc
    proc.kill()


@pytest.fixture
def es_client(embedding_hub_proc):
    client = es.EmbeddingHubClient()
    yield client
    client.close()


def test_set_get(es_client):
    space = uuid.uuid4()
    emb = [1, 2, 3]
    es_client.create_space(space, 3)
    es_client.set(space, "a", emb)
    assert es_client.get(space, "a") == emb


def test_immutable_set(es_client):
    space = uuid.uuid4()
    emb = [1, 2, 3]
    es_client.create_space(space, 3)
    es_client.set(space, "a", emb)
    assert es_client.get(space, "a") == emb
    es_client.freeze_space(space)
    with pytest.raises(TypeError):
        es_client.set(space, "a", emb)


def test_multiset_get(es_client):
    space = uuid.uuid4()
    embs = {
        "a": [1, 2, 3],
        "b": [3, 2, 1],
    }
    es_client.create_space(space, 3)
    es_client.multiset(space, embs)
    for key, emb in embs.items():
        assert es_client.get(space, key) == emb


def test_multiset_multiget(es_client):
    space = uuid.uuid4()
    embs = {
        "a": [1, 2, 3],
        "b": [3, 2, 1],
    }
    es_client.create_space(space, 3)
    es_client.multiset(space, embs)
    resp_embs = es_client.multiget(space, embs.keys())
    resp_emb_dict = {key: val for key, val in zip(embs.keys(), resp_embs)}
    assert embs == resp_emb_dict


def test_multi_space(es_client):
    key = "key"
    embs = {
        "a": [1, 2, 3],
        "b": [3, 2, 1],
    }
    for space in embs.keys():
        es_client.create_space(space, 3)
    for space, emb in embs.items():
        es_client.set(space, key, emb)

    # This is purposely in two loops. One sets the entire state before
    # querying it.
    for space, emb in embs.items():
        assert es_client.get(space, key) == emb


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
