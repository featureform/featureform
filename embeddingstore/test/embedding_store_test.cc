/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#include <vector>

#include "embeddingstore/embedding_store.h"

#include <gtest/gtest.h>

using featureform::embedding::EmbeddingStore;

namespace {

TEST(SimpleSpace, TestPutGet) {
  auto es = EmbeddingStore::load_or_create("testdb");
  auto space = es->create_space("test", 3);
  auto expected = std::vector<float>{1.1, 1.2, 1.3};
  space->set("a", expected);
  auto got_space = es->get_space("test").value();
  ASSERT_EQ(got_space->get("a"), expected);
}

}
