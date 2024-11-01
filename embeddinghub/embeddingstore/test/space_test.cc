/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#include "embeddingstore/space.h"

#include <gtest/gtest.h>

#include <vector>

using featureform::embedding::Space;

namespace {

TEST(SimpleSpace, TestCreateGetVersion) {
  auto space =
      Space::load_or_create("SimpleSpace_TestCreateGetVersion", "test");
  auto version = space->create_version("v1", 3);
  auto expected = std::vector<float>{1.1, 1.2, 1.3};
  version->set("a", expected);
  auto got_version = space->get_version("v1").value();
  ASSERT_EQ(got_version->get("a"), expected);
}

}  // namespace
