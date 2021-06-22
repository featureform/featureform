/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#include "embeddingstore/metadata.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <vector>

using featureform::embedding::EmbeddingStoreMetadata;

namespace {

TEST(SimpleMetadata, CreateSpace) {
  auto exp_meta =
      EmbeddingStoreMetadata::load_or_create("SimpleMetadata_CreateSpace");
  ASSERT_TRUE(exp_meta) << exp_meta.error()->to_string();
  auto meta = *exp_meta;
  auto initial_err = meta->create_space("test", 3);
  ASSERT_EQ(initial_err, nullptr);
  auto second_err = meta->create_space("test", 3);
  ASSERT_NE(second_err, nullptr);
  ASSERT_EQ(second_err->type(), "SpaceAlreadyExists");
}

TEST(SimpleMetadata, HasSpace) {
  auto exp_meta =
      EmbeddingStoreMetadata::load_or_create("SimpleMetadata_HasSpace");
  ASSERT_TRUE(exp_meta) << exp_meta.error()->to_string();
  auto meta = *exp_meta;
  auto exp_has = meta->has_space("test");
  ASSERT_TRUE(exp_has) << exp_has.error()->to_string();
  ASSERT_FALSE(*exp_has);
  auto initial_err = meta->create_space("test", 3);
  ASSERT_EQ(initial_err, nullptr);
  exp_has = meta->has_space("test");
  ASSERT_TRUE(exp_has) << exp_has.error()->to_string();
  ASSERT_TRUE(*exp_has);
}

TEST(SimpleMetadata, GetSpace) {
  auto exp_meta =
      EmbeddingStoreMetadata::load_or_create("SimpleMetadata_GetSpace");
  ASSERT_TRUE(exp_meta) << exp_meta.error()->to_string();
  auto meta = *exp_meta;
  auto exp_get = meta->get_space("test");
  ASSERT_FALSE(exp_get) << "Succeeded to get non-existant space";
  auto initial_err = meta->create_space("test", 3);
  ASSERT_EQ(initial_err, nullptr);
  exp_get = meta->get_space("test");
  ASSERT_TRUE(exp_get) << exp_get.error()->to_string();
  auto space_meta = *exp_get;
  ASSERT_EQ(space_meta.name(), "test");
  ASSERT_TRUE(std::filesystem::path(space_meta.path()).is_absolute());
}

TEST(SimpleMetadata, HasVersion) {
  auto exp_meta =
      EmbeddingStoreMetadata::load_or_create("SimpleMetadata_HasVersion");
  ASSERT_TRUE(exp_meta) << exp_meta.error()->to_string();
  auto meta = *exp_meta;
  auto exp_has = meta->has_version("test", "initial");
  ASSERT_TRUE(exp_has) << exp_has.error()->to_string();
  ASSERT_FALSE(*exp_has);
  auto initial_err = meta->create_space("test", 3);
  ASSERT_EQ(initial_err, nullptr);
  exp_has = meta->has_version("test", "initial");
  ASSERT_TRUE(exp_has) << exp_has.error()->to_string();
  ASSERT_TRUE(*exp_has);
}

TEST(SimpleMetadata, GetVersion) {
  auto exp_meta =
      EmbeddingStoreMetadata::load_or_create("SimpleMetadata_GetVersion");
  ASSERT_TRUE(exp_meta.has_value()) << exp_meta.error()->to_string();
  auto meta = *exp_meta;
  auto create_space_err = meta->create_space("test", 3);
  ASSERT_EQ(create_space_err, nullptr);
  auto exp_get = meta->get_version("test", "initial");
  ASSERT_TRUE(exp_get.has_value()) << exp_get.error()->to_string();
  auto version_meta = *exp_get;
  ASSERT_EQ(version_meta.space(), "test");
  ASSERT_EQ(version_meta.name(), "initial");
  ASSERT_EQ(version_meta.dims(), 3);
  ASSERT_TRUE(std::filesystem::path(version_meta.path()).is_absolute());

  auto create_ver_err = meta->create_version("test", "v2", 5);
  ASSERT_EQ(create_ver_err, nullptr);
  auto exp_get_v2 = meta->get_version("test", "v2");
  version_meta = *exp_get_v2;
  ASSERT_EQ(version_meta.space(), "test");
  ASSERT_EQ(version_meta.name(), "v2");
  ASSERT_EQ(version_meta.dims(), 5);
  ASSERT_TRUE(std::filesystem::path(version_meta.path()).is_absolute());
}

}  // namespace
