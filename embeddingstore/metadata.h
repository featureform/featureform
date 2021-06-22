/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#pragma once

#include <filesystem>
#include <memory>
#include <shared_mutex>
#include <sstream>

#include "embeddingstore/embedding_store_meta.pb.h"
#include "error.h"
#include "rocksdb/db.h"
#include "tl/expected.h"

namespace featureform {

namespace embedding {

class SpaceMeta {
 public:
  SpaceMeta(std::filesystem::path base_path, std::string name);
  explicit SpaceMeta(proto::SpaceMetaValue proto);
  explicit operator proto::SpaceMetaValue() const;
  std::string name() const { return name_; }
  std::filesystem::path path() const { return path_; }
  std::string serialize() const;

 private:
  std::filesystem::path path_;
  std::string name_;
};

class VersionMeta {
 public:
  VersionMeta(const SpaceMeta& space, std::string name, size_t dims);
  explicit VersionMeta(proto::VersionMetaValue proto);
  explicit operator proto::VersionMetaValue() const;
  std::string space() const { return space_; }
  std::string name() const { return name_; }
  size_t dims() const { return dims_; }
  std::filesystem::path path() const { return path_; }
  std::string serialize() const;

 private:
  std::filesystem::path path_;
  std::string space_;
  std::string name_;
  size_t dims_;
};

class EmbeddingStoreMetadata {
 public:
  static tl::expected<std::shared_ptr<EmbeddingStoreMetadata>, Error>
  load_or_create(std::string path);
  tl::expected<std::vector<VersionMeta>, Error> get_versions(
      const std::string& space);
  tl::expected<VersionMeta, Error> get_latest_version(const std::string& space);
  Error create_space(const std::string& space, size_t dims) {
    return create_space(space, "initial", dims);
  }
  Error create_space(const std::string& space, const std::string& version,
                     size_t dims);
  tl::expected<SpaceMeta, Error> get_space(const std::string& space);
  tl::expected<bool, Error> has_space(const std::string& space);
  Error create_version(const std::string& space, const std::string& version,
                       size_t dims);
  tl::expected<VersionMeta, Error> get_version(const std::string& space,
                                               const std::string& version);
  tl::expected<bool, Error> has_version(const std::string& space,
                                        const std::string& version);

 private:
  EmbeddingStoreMetadata(std::filesystem::path base_path,
                         std::unique_ptr<rocksdb::DB> db);

  template <typename MetaType, typename... MetaTypes>
  Error rocksdb_put(const MetaType& meta, const MetaTypes&... args) {
    auto key = serialize_key(meta);
    auto value = meta.serialize();
    auto status = db_->Put(rocksdb::WriteOptions(), key, value);
    if (!status.ok()) {
      return std::make_unique<RocksDBError>(status);
    }
    return rocksdb_put(args...);
  }

  Error rocksdb_put() { return nullptr; }

  std::string serialize_key(const VersionMeta& version);
  std::string serialize_key(const SpaceMeta& space);
  std::string serialize_space_key(const std::string& space);
  std::string serialize_version_key(const std::string& space,
                                    const std::string& version);
  tl::expected<bool, Error> rocksdb_has_key(const std::string& key);
  std::filesystem::path base_path_;
  std::unique_ptr<rocksdb::DB> db_;
  std::shared_mutex mtx_;
};

class SpaceAlreadyExists : public ErrorBase {
 public:
  SpaceAlreadyExists(const std::string& space) : name_{space} {};
  std::string to_string() const override {
    std::ostringstream stream;
    stream << "Space " << name_ << " already exists." << std::endl;
    return stream.str();
  }

  std::string type() const override { return "SpaceAlreadyExists"; }

 private:
  std::string name_;
};

class SpaceNotFound : public ErrorBase {
 public:
  SpaceNotFound(const std::string& space) : name_{space} {};
  std::string to_string() const override {
    std::ostringstream stream;
    stream << "Space " << name_ << " not found." << std::endl;
    return stream.str();
  }

  std::string type() const override { return "SpaceNotFound"; }

 private:
  std::string name_;
};

class VersionAlreadyExists : public ErrorBase {
 public:
  VersionAlreadyExists(const std::string& space, const std::string& version)
      : space_{space}, name_{version} {};
  std::string to_string() const override {
    std::ostringstream stream;
    stream << "Space " << space_ << " already contains version " << name_ << "."
           << std::endl;
    return stream.str();
  }

  std::string type() const override { return "VersionAlreadyExists"; }

 private:
  std::string space_;
  std::string name_;
};

class InvalidName : public ErrorBase {
 public:
  static std::unique_ptr<InvalidName> check(std::string name);
  static std::unique_ptr<InvalidName> check(
      std::initializer_list<std::string> names);
  InvalidName(std::string err_msg) : err_msg_{err_msg} {};
  std::string to_string() const override { return "Invalid Name: " + err_msg_; }

  std::string type() const override { return "InvalidName"; }

 private:
  std::string err_msg_;
};
}  // namespace embedding
}  // namespace featureform
