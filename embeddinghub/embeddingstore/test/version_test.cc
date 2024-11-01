/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#include "embeddingstore/version.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

using featureform::embedding::Version;

namespace {

TEST(SimpleVersion, TestPutGet) {
  auto store =
      Version::load_or_create("SimpleVersion_TestPutGet", "test", "v1", 3);
  auto set_err = store->set("a", std::vector<float>{1.1, 1.2, 1.3});
  ASSERT_EQ(set_err, nullptr);
  std::vector<float> expected{1.1, 1.2, 1.3};
  ASSERT_EQ(store->get("a"), expected);
}

TEST(SimpleVersion, TestImmutable) {
  auto store =
      Version::load_or_create("SimpleVersion_TestImmutable", "test", "v1", 3);
  std::vector<float> expected{1.1, 1.2, 1.3};
  auto set_err = store->set("a", std::vector<float>{1.1, 1.2, 1.3});
  ASSERT_EQ(set_err, nullptr);
  ASSERT_EQ(store->get("a"), expected);
  store->make_immutable();
  set_err = store->set("a", std::vector<float>{1.1, 1.2, 1.3});
  ASSERT_NE(set_err, nullptr);
  ASSERT_EQ(store->get("a"), expected);
}

TEST(SimpleVersion, TestGetters) {
  auto space = "test";
  auto name = "v1";
  auto dims = 3;
  auto store = Version::load_or_create("SimpleVersion_TestGetters", space, name,
                                       dims, false);
  ASSERT_EQ(store->space(), space);
  ASSERT_EQ(store->name(), name);
  ASSERT_EQ(store->dims(), 3);
  ASSERT_FALSE(store->immutable());
}
}  // namespace
