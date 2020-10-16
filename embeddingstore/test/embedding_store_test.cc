/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#include <memory>
#include <vector>

#include <gtest/gtest.h>
#include "embeddingstore/embedding_store.h"

namespace {

using featureform::embeddings::EmbeddingStore;

TEST(SimpleEmbeddingStore, TestPutGet) {
  const auto store =
      std::make_shared<EmbeddingStore<std::string, std::vector<float>>>();
  store->set("a", std::vector<float>{1.1, 1.2, 1.3});
  std::vector<float> expected{1.1, 1.2, 1.3};
  ASSERT_EQ(store->get("a"), expected);
}
}
