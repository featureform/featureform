/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#include "metadata.h"

#include <filesystem>
#include <shared_mutex>

#include "embeddingstore/embedding_store_meta.pb.h"
#include "rocksdb/db.h"

namespace featureform {

namespace embedding {

tl::expected<std::shared_ptr<EmbeddingStoreMetadata>, Error>
EmbeddingStoreMetadata::load_or_create(std::string path) {
  std::filesystem::path base_path(path);
  base_path = std::filesystem::absolute(path);
  base_path /= ".embeddingstore_data";
  auto metadata_path = base_path / "__metadata";
  std::filesystem::create_directories(metadata_path);
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::DB* db_ptr;
  rocksdb::Status status = rocksdb::DB::Open(options, metadata_path, &db_ptr);
  if (!status.ok()) {
    return tl::unexpected(std::make_unique<RocksDBError>(status));
  }
  std::unique_ptr<rocksdb::DB> db(db_ptr);
  return std::shared_ptr<EmbeddingStoreMetadata>(
      new EmbeddingStoreMetadata(base_path, std::move(db)));
}

EmbeddingStoreMetadata::EmbeddingStoreMetadata(std::filesystem::path base_path,
                                               std::unique_ptr<rocksdb::DB> db)
    : base_path_{base_path}, db_{std::move(db)}, mtx_{} {}

Error EmbeddingStoreMetadata::create_space(const std::string& space,
                                           const std::string& version,
                                           size_t dims) {
  if (auto invalid = InvalidName::check({space, version}); invalid) {
    return invalid;
  }

  auto space_meta = SpaceMeta(base_path_, space);
  auto space_key = serialize_key(space_meta);
  std::unique_lock<std::shared_mutex> lock(mtx_);
  auto exp = rocksdb_has_key(space_key);
  if (!exp) {
    return std::move(exp.error());
  } else if (bool has_key = *exp; has_key) {
    return std::make_unique<SpaceAlreadyExists>(space);
  }
  auto version_meta = VersionMeta(space_meta, version, dims);
  return rocksdb_put(space_meta, version_meta);
}

tl::expected<SpaceMeta, Error> EmbeddingStoreMetadata::get_space(
    const std::string& space) {
  if (auto invalid = InvalidName::check(space); invalid) {
    return tl::unexpected(std::move(invalid));
  }
  std::shared_lock<std::shared_mutex> lock(mtx_);
  std::string serialized;
  auto status = db_->Get(rocksdb::ReadOptions(), space, &serialized);
  if (!status.ok()) {
    return tl::unexpected(std::make_unique<RocksDBError>(status));
  }
  auto entry = proto::SpaceMetaValue();
  entry.ParseFromString(serialized);
  return static_cast<SpaceMeta>(entry);
}

tl::expected<bool, Error> EmbeddingStoreMetadata::has_space(
    const std::string& space) {
  if (auto invalid = InvalidName::check(space); invalid) {
    return tl::unexpected(std::move(invalid));
  }
  std::shared_lock<std::shared_mutex> lock(mtx_);
  return rocksdb_has_key(space);
}

Error EmbeddingStoreMetadata::create_version(const std::string& space,
                                             const std::string& version,
                                             size_t dims) {
  if (auto invalid = InvalidName::check({space, version}); invalid) {
    return invalid;
  }
  auto space_meta = SpaceMeta(base_path_, space);
  auto space_key = serialize_key(space_meta);
  std::unique_lock<std::shared_mutex> lock(mtx_);
  auto exp = rocksdb_has_key(space_key);
  if (!exp) {
    return std::move(exp.error());
  } else if (bool has_key = *exp; !has_key) {
    return std::make_unique<SpaceNotFound>(space);
  }
  auto version_meta = VersionMeta(space_meta, version, dims);
  exp = rocksdb_has_key(serialize_key(version_meta));
  if (!exp.has_value()) {
    return std::move(exp.error());
  } else if (bool has_key = *exp; has_key) {
    return std::make_unique<VersionAlreadyExists>(space, version);
  }
  return rocksdb_put(version_meta);
}

tl::expected<VersionMeta, Error> EmbeddingStoreMetadata::get_version(
    const std::string& space, const std::string& version) {
  if (auto invalid = InvalidName::check(space); invalid) {
    return tl::unexpected(std::move(invalid));
  }
  std::shared_lock<std::shared_mutex> lock(mtx_);
  auto version_key = serialize_version_key(space, version);
  std::string serialized;
  auto status = db_->Get(rocksdb::ReadOptions(), version_key, &serialized);
  if (!status.ok()) {
    return tl::unexpected(std::make_unique<RocksDBError>(status));
  }
  auto entry = proto::VersionMetaValue();
  entry.ParseFromString(serialized);
  return static_cast<VersionMeta>(entry);
}

tl::expected<bool, Error> EmbeddingStoreMetadata::has_version(
    const std::string& space, const std::string& version) {
  if (auto invalid = InvalidName::check({space, version}); invalid) {
    return tl::unexpected(std::move(invalid));
  }
  auto key = serialize_version_key(space, version);
  std::shared_lock<std::shared_mutex> lock(mtx_);
  return rocksdb_has_key(key);
}

// I CAN FINISH THE VERSION FNS AND FIT THIS INTO EMBEDDING_STORE.H, THEN I
// GOTTA ADD IMMUTABILITY.

std::string EmbeddingStoreMetadata::serialize_key(const VersionMeta& version) {
  std::ostringstream stream;
  stream << version.space() << "__" << version.name();
  return stream.str();
}

std::string EmbeddingStoreMetadata::serialize_key(const SpaceMeta& space) {
  return space.name();
}

std::string EmbeddingStoreMetadata::serialize_version_key(
    const std::string& space, const std::string& version) {
  std::ostringstream stream;
  stream << space << "__" << version;
  return stream.str();
}

std::string EmbeddingStoreMetadata::serialize_space_key(
    const std::string& space) {
  return space;
}

tl::expected<bool, Error> EmbeddingStoreMetadata::rocksdb_has_key(
    const std::string& key) {
  std::string temp;
  auto status = db_->Get(rocksdb::ReadOptions(), key, &temp);
  bool exists = status.ok();
  bool not_exists = status.IsNotFound();
  bool err = !exists && !not_exists;
  if (err) {
    return tl::unexpected(std::make_unique<RocksDBError>(status));
  }
  return exists;
}

SpaceMeta::SpaceMeta(std::filesystem::path base_path, std::string name)
    : path_{base_path / name}, name_{name} {}

SpaceMeta::SpaceMeta(proto::SpaceMetaValue proto)
    : path_{proto.path()}, name_{proto.name()} {}

SpaceMeta::operator proto::SpaceMetaValue() const {
  auto pb = proto::SpaceMetaValue();
  pb.set_path(path_);
  pb.set_name(name_);
  return pb;
}

std::string SpaceMeta::serialize() const {
  std::string serialized;
  static_cast<proto::SpaceMetaValue>(*this).SerializeToString(&serialized);
  return serialized;
}

VersionMeta::VersionMeta(const SpaceMeta& space, std::string name, size_t dims)
    : path_{space.path() / name},
      space_{space.name()},
      name_{name},
      dims_{dims} {}

VersionMeta::VersionMeta(proto::VersionMetaValue proto)
    : path_{proto.path()},
      space_{proto.space()},
      name_{proto.name()},
      dims_{proto.dims()} {}

VersionMeta::operator proto::VersionMetaValue() const {
  auto pb = proto::VersionMetaValue();
  pb.set_path(path_);
  pb.set_space(space_);
  pb.set_name(name_);
  pb.set_dims(dims_);
  return pb;
}

std::string VersionMeta::serialize() const {
  std::string serialized;
  static_cast<proto::VersionMetaValue>(*this).SerializeToString(&serialized);
  return serialized;
}

std::unique_ptr<InvalidName> InvalidName::check(
    std::initializer_list<std::string> names) {
  for (auto name : names) {
    auto err = InvalidName::check(name);
    if (err != nullptr) {
      return err;
    }
  }
  return nullptr;
}

std::unique_ptr<InvalidName> InvalidName::check(std::string name) {
  bool contains_slash = name.find("/") != std::string::npos;
  bool contains_double_underscore = name.find("__") != std::string::npos;
  if (contains_slash) {
    return std::make_unique<InvalidName>(
        "Name cannot contain a forward slash.");
  }
  if (contains_double_underscore) {
    return std::make_unique<InvalidName>(
        "Name cannot contain a double underscore.");
  }
  return nullptr;
}

}  // namespace embedding
}  // namespace featureform
