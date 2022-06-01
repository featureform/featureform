/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#include "storage.h"

#include "iterator.h"
#include "rocksdb/db.h"

namespace featureform {

namespace embedding {

std::shared_ptr<EmbeddingStorage> EmbeddingStorage::load_or_create(
    std::string path, int dims) {
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::DB* db_ptr;
  rocksdb::Status status = rocksdb::DB::Open(options, path, &db_ptr);
  std::shared_ptr<rocksdb::DB> db(db_ptr);
  return std::unique_ptr<EmbeddingStorage>(
      new EmbeddingStorage(std::move(db), dims));
}

EmbeddingStorage::EmbeddingStorage(std::shared_ptr<rocksdb::DB> db, int dims)
    : serializer_{}, db_(std::move(db)), dims_(dims) {}

void EmbeddingStorage::set(std::string key, std::vector<float> val) {
  db_->Put(rocksdb::WriteOptions(), key, serializer_.serialize(val));
}

std::vector<float> EmbeddingStorage::get(const std::string& key) const {
  std::string serialized;
  db_->Get(rocksdb::ReadOptions(), key, &serialized);
  return serializer_.deserialize(serialized);
}

Iterator EmbeddingStorage::iterator() const { return Iterator(db_); }

}  // namespace embedding
}  // namespace featureform
