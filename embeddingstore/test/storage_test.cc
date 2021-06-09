/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#include "embeddingstore/storage.h"

#include <gtest/gtest.h>

#include <iostream>
#include <memory>
#include <vector>

using featureform::embedding::EmbeddingStorage;

namespace {

TEST(EmbeddingStorage, TestSetGet) {
  auto storage = EmbeddingStorage::load_or_create("test.db", 3);
  auto a_key = "a";
  auto a_vec = std::vector<float>{0, 1, 0};
  storage->set(a_key, a_vec);
  ASSERT_EQ(storage->get(a_key), a_vec);
}

TEST(EmbeddingStorage, TestIter) {
  std::shared_ptr<EmbeddingStorage> storage =
      EmbeddingStorage::load_or_create("test.db", 3);
  std::unordered_map<std::string, std::vector<float>> vals = {
      {"a", std::vector<float>{0, 1, 0}},
      {"b", std::vector<float>{1, 0, 0}},
  };
  for (auto args : vals) {
    storage->set(args.first, args.second);
  }
  auto iter = EmbeddingStorage::Iterator(storage);
  auto ctr = 0;
  while (iter.scan()) {
    auto key = iter.key();
    ASSERT_EQ(iter.value(), vals[key]);
    ctr++;
  }
  ASSERT_EQ(ctr, vals.size());
}

}  // namespace
