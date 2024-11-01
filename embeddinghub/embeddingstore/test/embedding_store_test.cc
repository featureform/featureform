/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#include "embeddingstore/embedding_store.h"

#include <gtest/gtest.h>

#include <vector>

using featureform::embedding::EmbeddingHub;

namespace {

TEST(SimpleSpace, TestPutGet) {
  auto es = EmbeddingHub::load_or_create("testdb");
  auto space = es->create_space("test");
  auto got_space = es->get_space("test").value();
  ASSERT_EQ(*space, *got_space);
}

}  // namespace
