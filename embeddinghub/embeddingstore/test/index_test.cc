/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#include "embeddingstore/index.h"

#include <gtest/gtest.h>

#include <iostream>
#include <memory>
#include <vector>

using featureform::embedding::ANNIndex;

namespace {

TEST(SimpleANNIndex, TestSimpleANN) {
  auto idx = std::make_unique<ANNIndex>(3);
  auto a_vec = std::vector<float>{0, 1, 0};
  idx->set("a", a_vec);
  idx->set("b", std::vector<float>{1, 1, 0});
  idx->set("c", std::vector<float>{1, 0, 0});
  const auto nearest = idx->approx_nearest(a_vec, 1);
  std::vector<std::string> expected{"a"};
  ASSERT_EQ(nearest, expected);
}

TEST(SimpleANNIndex, TestMultiANN) {
  auto idx = std::make_unique<ANNIndex>(3);
  auto a_vec = std::vector<float>{0, 1, 0};
  idx->set("a", a_vec);
  idx->set("b", std::vector<float>{1, 1, 0});
  idx->set("c", std::vector<float>{1, 0, 0});
  const auto nearest = idx->approx_nearest(a_vec, 2);
  std::vector<std::string> expected{"a", "b"};
  ASSERT_EQ(nearest, expected);
}

TEST(SimpleANNIndex, TestUpdateANN) {
  auto idx = std::make_unique<ANNIndex>(3);
  auto a_vec = std::vector<float>{0, 1, 0};
  idx->set("a", a_vec);
  idx->set("b", std::vector<float>{1, 1, 0});
  idx->set("c", std::vector<float>{1, 0, 0});
  idx->set("a", std::vector<float>{0, -1, 0});
  const auto nearest = idx->approx_nearest(a_vec, 1);
  std::vector<std::string> expected{"b"};
  ASSERT_EQ(nearest, expected);
}

TEST(SimpleANNIndex, TestANN0Items) {
  auto idx = std::make_unique<ANNIndex>(3);
  auto a_vec = std::vector<float>{0, 1, 0};
  idx->set("a", a_vec);
  idx->set("b", std::vector<float>{1, 1, 0});
  idx->set("c", std::vector<float>{1, 0, 0});
  const auto nearest = idx->approx_nearest(a_vec, 0);
  std::vector<std::string> expected{};
  ASSERT_EQ(nearest, expected);
}
}  // namespace
