/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#include "embeddingstore/embedding_store.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

using featureform::embedding::EmbeddingStore;

namespace {

TEST(SimpleEmbeddingStore, TestPutGet) {
  auto store = EmbeddingStore::load_or_create("test.putget", "test.putget", 3);
  store->set("a", std::vector<float>{1.1, 1.2, 1.3});
  std::vector<float> expected{1.1, 1.2, 1.3};
  ASSERT_EQ(store->get("a"), expected);
}

TEST(SimpleEmbeddingStore, TestPutGetWithIndex) {
  auto store = EmbeddingStore::load_or_create_with_index(
      "test.putget.index", "test.putget.index", 3);
  store->set("a", std::vector<float>{1.1, 1.2, 1.3});
  std::vector<float> expected{1.1, 1.2, 1.3};
  ASSERT_EQ(store->get("a"), expected);
}

TEST(SimpleEmbeddingStore, TestSearch) {
  auto store = EmbeddingStore::load_or_create_with_index("test.search",
                                                         "test.search", 3);
  store->set("a", std::vector<float>{1.1, 1.2, 1.3});
  store->set("b", std::vector<float>{1.1, 1.3, 1.3});
  store->set("c", std::vector<float>{0., 0.2, 0.3});

  auto index = store->get_or_create_index();
  auto out = store->get_neighbors("a", 2);
  std::vector<std::string> out_keys;
  for (const auto i : out) {
    std::cout << i.key() << ' ';
    out_keys.push_back(i.key());
  }
  std::vector<std::string> expected{"b", "c"};
  ASSERT_EQ(out_keys, expected);
}

TEST(SimpleEmbeddingStore, TestErase) {
  auto store =
      EmbeddingStore::load_or_create_with_index("test.erase", "test.erase", 3);
  store->set("a", std::vector<float>{1.1, 1.2, 1.3});
  std::vector<float> expected{1.1, 1.2, 1.3};
  ASSERT_EQ(store->get("a"), expected);

  store->erase();

  store =
      EmbeddingStore::load_or_create_with_index("test.erase", "test.erase", 3);
  ASSERT_NE(store->get("a"), expected);
}

}  // namespace
