/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#include <memory>
#include <vector>

#include <gtest/gtest.h>
#include "embeddingstore/embedding_store.h"

using featureform::embedding::EmbeddingStore;

namespace {

TEST(SimpleEmbeddingStore, TestPutGet) {
  auto store = EmbeddingStore::load_or_create("test.abc", 3);
  store->set("a", std::vector<float>{1.1, 1.2, 1.3});
  std::vector<float> expected{1.1, 1.2, 1.3};
  ASSERT_EQ(store->get("a"), expected);
}
}
