# This Source Code Form is subject to the terms of the Mozilla Public
# License, v.2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

import pytest
import sys

from sdk.python.offlinehub import Index


@pytest.fixture
def init_embeddings():
    return [
        ("a", [1, 0]),
        ("b", [0, 1]),
        ("c", [-1, -1]),
    ]


@pytest.fixture
def init_args():
    return ([
        ("a", [1, 0]),
        ("b", [0, 1]),
        ("c", [-1, -1]),
    ], 2)


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
    assert index.nearest_neighbor("a", 1) == ["b"]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
