/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#include <memory>
#include <vector>

#include "embeddingstore/space.h"

#include <gtest/gtest.h>

using featureform::embedding::Space;

namespace {

TEST(SimpleSpace, TestPutGet) {
  auto store = Space::load_or_create("test.abc", "test", 3);
  store->set("a", std::vector<float>{1.1, 1.2, 1.3});
  std::vector<float> expected{1.1, 1.2, 1.3};
  ASSERT_EQ(store->get("a"), expected);
}

TEST(SimpleSpace, TestGetters) {
  auto name = "test";
  auto dims = 3;
  auto store = Space::load_or_create("test.abc", name, dims);
  ASSERT_EQ(store->name(), name);
  ASSERT_EQ(store->dims(), 3);
}
}
