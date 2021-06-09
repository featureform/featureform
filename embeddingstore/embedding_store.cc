/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#include "embedding_store.h"

#include <filesystem>

#include "embeddingstore/embedding_store_meta.pb.h"
#include "rocksdb/db.h"

namespace featureform {

namespace embedding {

std::shared_ptr<EmbeddingStore> EmbeddingStore::load_or_create(
    std::string path) {
  std::filesystem::path metadata_path = path;
  std::filesystem::create_directories(path);
  metadata_path /= "metadata";
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::DB* db_ptr;
  rocksdb::Status status = rocksdb::DB::Open(options, metadata_path, &db_ptr);
  std::unique_ptr<rocksdb::DB> db(db_ptr);
  return std::shared_ptr<EmbeddingStore>(
      new EmbeddingStore(metadata_path, std::move(db)));
}

EmbeddingStore::EmbeddingStore(std::filesystem::path base_path,
                               std::unique_ptr<rocksdb::DB> db)
    : base_path_{base_path}, db_{std::move(db)}, loaded_spaces_{} {}

std::shared_ptr<Space> EmbeddingStore::create_space(const std::string& name,
                                                    int dims) {
  if (is_space_loaded(name)) {
    return loaded_spaces_.at(name);
  }
  auto entry = proto::SpaceEntry();
  auto path = base_path_ / name;
  entry.set_path(path);
  entry.set_name(name);
  entry.set_dims(dims);
  std::string serialized;
  entry.SerializeToString(&serialized);
  db_->Put(rocksdb::WriteOptions(), name, serialized);
  auto space = Space::load_or_create(path, name, dims);
  space->create_ann_index();
  loaded_spaces_.emplace(name, space);
  return space;
}

std::optional<std::shared_ptr<Space>> EmbeddingStore::get_space(
    const std::string& name) {
  if (is_space_loaded(name)) {
    return loaded_spaces_.at(name);
  }
  std::string serialized;
  auto status = db_->Get(rocksdb::ReadOptions(), name, &serialized);
  if (!status.ok()) {
    return std::nullopt;
  }
  auto entry = proto::SpaceEntry();
  entry.ParseFromString(serialized);
  auto space = Space::load_or_create(entry.path(), entry.name(), entry.dims());
  loaded_spaces_.emplace(name, space);
  return std::optional{space};
}

bool EmbeddingStore::is_space_loaded(const std::string& name) const {
  return loaded_spaces_.find(name) != loaded_spaces_.end();
}

}  // namespace embedding
}  // namespace featureform
