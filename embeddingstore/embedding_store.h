/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#pragma once

#include <filesystem>
#include <memory>
#include <optional>
#include <unordered_map>

#include "index.h"
#include "space.h"
#include "storage.h"

namespace featureform {

namespace embedding {

class EmbeddingStore {
 public:
  static std::shared_ptr<EmbeddingStore> load_or_create(std::string path);
  std::optional<std::shared_ptr<Space>> get_space(const std::string& name);
  std::shared_ptr<Space> create_space(const std::string& name, int dims);

 private:
  EmbeddingStore(std::filesystem::path base_path,
                 std::unique_ptr<rocksdb::DB> db);
  bool is_space_loaded(const std::string& name) const;
  std::filesystem::path base_path_;
  std::unique_ptr<rocksdb::DB> db_;
  std::unordered_map<std::string, std::shared_ptr<Space>> loaded_spaces_;
};
}  // namespace embedding
}  // namespace featureform
