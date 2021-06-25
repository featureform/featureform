/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#pragma once

#include <filesystem>
#include <memory>
#include <optional>
#include <unordered_map>

#include "version.h"

namespace featureform {

namespace embedding {

class Space {
 public:
  static std::shared_ptr<Space> load_or_create(
      const std::filesystem::path& path, const std::string& name);
  std::optional<std::shared_ptr<Version>> get_version(const std::string& name);
  std::shared_ptr<Version> create_version(const std::string& name, int dims);

 private:
  Space(std::filesystem::path base_path, std::unique_ptr<rocksdb::DB> db);
  bool is_version_loaded(const std::string& name) const;
  std::filesystem::path base_path_;
  std::string name_;
  std::unique_ptr<rocksdb::DB> db_;
  std::unordered_map<std::string, std::shared_ptr<Version>> loaded_versions_;
};
}  // namespace embedding
}  // namespace featureform
