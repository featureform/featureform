/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#pragma once

#include <memory>

#include "rocksdb/db.h"

namespace featureform {

namespace embedding {

class EmbeddingStorage {
 public:
  static std::unique_ptr<EmbeddingStorage> load_or_create(std::string path,
                                                          int dims);
  EmbeddingStorage() = delete;
  void set(std::string key, std::vector<float> value);
  std::vector<float> get(const std::string& key) const;
  const bool check_exists(const std::string& key) const;
  std::unordered_map<std::string, std::vector<float>> get_all() const;
  void backup_to(const std::string& filepath) const;
  void proto_out(const std::string& filepath) const;
  void proto_in(const std::string& filepath) const;
  void close();
  void erase();

 private:
  EmbeddingStorage(std::unique_ptr<rocksdb::DB> DB, std::string path, int dims);
  std::unique_ptr<rocksdb::DB> db_;
  std::string path_;
  int dims_;
};

}  // namespace embedding
}  // namespace featureform
