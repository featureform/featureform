/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#pragma once

#include <memory>
#include <optional>

#include "iterator.h"
#include "rocksdb/db.h"
#include "serializer.h"

namespace featureform {

namespace embedding {

class EmbeddingStorage {
 public:
  static std::shared_ptr<EmbeddingStorage> load_or_create(std::string path,
                                                          int dims);
  EmbeddingStorage() = delete;
  void set(std::string key, std::vector<float> value);
  std::vector<float> get(const std::string& key) const;
  Iterator iterator() const;

 private:
  EmbeddingStorage(std::shared_ptr<rocksdb::DB> DB, int dims);
  ProtoSerializer serializer_;
  std::shared_ptr<rocksdb::DB> db_;
  int dims_;
};
}  // namespace embedding
}  // namespace featureform
