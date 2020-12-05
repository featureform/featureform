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
  static std::unique_ptr<EmbeddingStorage> load_or_create(std::string path, int dims);
  EmbeddingStorage() = delete;
  void set(std::string key, std::vector<float> value);
  std::vector<float> get(const std::string& key) const;
 private:
  EmbeddingStorage(std::unique_ptr<rocksdb::DB> DB, int dims);
  std::unique_ptr<rocksdb::DB> db_;
  int dims_;
};
}
}
