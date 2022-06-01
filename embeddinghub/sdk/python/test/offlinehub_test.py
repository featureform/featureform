# This Source Code Form is subject to the terms of the Mozilla Public
# License, v.2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

import pytest
import sys

from embeddinghub.sdk.python.offlinehub import Index


@pytest.fixture
def init_embeddings():
    return [
        ("a", [1, 0]),
        ("b", [0, 1]),
        ("c", [-1, -1]),
        ("d", [1, 1]),
    ]


@pytest.fixture
def init_args(init_embeddings):
    return (init_embeddings, 2)


def test_set_get_index():
    index = Index([], 3)
    emb = [1, 2, 3]
    index.set("a", emb)
    assert index.get("a") == emb


def test_get(init_args):
    index = Index(*init_args)
    assert index.get("a") == [1, 0]


def test_set(init_args):
    index = Index(*init_args)
    index.set("a", [5, 5])
    assert index.get("a") == [5, 5]


def test_multiset_multiget(init_args):
    index = Index(*init_args)
    embs = {
        "a": [3, 3],
        "b": [4, 4],
    }
    keys = ["a", "b", "c"]
    expected = [
        [3, 3],
        [4, 4],
        [-1, -1],
    ]
    index.multiset(embs)
    assert index.multiget(keys) == expected


def test_nn(init_args):
    index = Index(*init_args)
    assert index.nearest_neighbor(2, key="a") == ["d", "b"]


def test_capacity_set():
    index = Index([], 2)
    for key in list(range(1025)) * 2:
        index.set(str(key), [1, 1])
    assert index.size() == 1025


def test_capacity_init():
    embs = [(key, [1, 1]) for key in list(range(1028)) * 2]
    index = Index(embs, 2)
    assert index.size() == 1028


def test_capacity_multiset():
    index = Index([], 2)
    embs = [(key, [1, 1]) for key in list(range(1028)) * 2]
    for emb_chunk in chunks(embs, 4):
        index.multiset(emb_chunk)
    assert index.size() == 1028


def chunks(l, chunk_size):
    for i in range(0, len(l), chunk_size):
        yield l[i:i + chunk_size]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
