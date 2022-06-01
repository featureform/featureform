/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#include "space.h"

#include <filesystem>

#include "embeddingstore/embedding_store_meta.pb.h"
#include "rocksdb/db.h"

namespace featureform {

namespace embedding {

std::shared_ptr<Space> Space::load_or_create(const std::filesystem::path& path,
                                             const std::string& name) {
  std::filesystem::create_directories(path);
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::DB* db_ptr;
  rocksdb::Status status = rocksdb::DB::Open(options, path, &db_ptr);
  std::unique_ptr<rocksdb::DB> db(db_ptr);
  return std::shared_ptr<Space>(new Space(path, std::move(db)));
}

Space::Space(std::filesystem::path base_path, std::unique_ptr<rocksdb::DB> db)
    : base_path_{base_path}, db_{std::move(db)}, loaded_versions_{} {}

std::shared_ptr<Version> Space::create_version(const std::string& name,
                                               int dims) {
  if (is_version_loaded(name)) {
    return loaded_versions_.at(name);
  }
  auto space_name = name_;
  auto entry = proto::VersionEntry();
  auto path = base_path_ / name;
  entry.set_path(path);
  entry.set_space(space_name);
  entry.set_name(name);
  entry.set_dims(dims);
  std::string serialized;
  entry.SerializeToString(&serialized);
  auto key = space_name + " " + name;
  db_->Put(rocksdb::WriteOptions(), key, serialized);
  auto version = Version::load_or_create(path, space_name, name, dims);
  version->create_ann_index();
  loaded_versions_.emplace(name, version);
  return version;
}

std::optional<std::shared_ptr<Version>> Space::get_version(
    const std::string& name) {
  if (is_version_loaded(name)) {
    return loaded_versions_.at(name);
  }
  auto space_name = name_;
  auto key = space_name + " " + name;
  std::string serialized;
  auto status = db_->Get(rocksdb::ReadOptions(), key, &serialized);
  if (!status.ok()) {
    return std::nullopt;
  }
  auto entry = proto::VersionEntry();
  entry.ParseFromString(serialized);
  auto version = Version::load_or_create(entry.path(), entry.space(),
                                         entry.name(), entry.dims());
  version->create_ann_index();
  loaded_versions_.emplace(name, version);
  return std::optional{version};
}

bool Space::is_version_loaded(const std::string& name) const {
  return loaded_versions_.find(name) != loaded_versions_.end();
}

bool Space::operator==(const Space& other) const {
  return base_path_ == other.base_path_ && name_ == other.name_;
}

}  // namespace embedding
}  // namespace featureform
