/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#include "storage.h"

#include "embeddingstore/embedding_store.grpc.pb.h"
#include "rocksdb/db.h"
#include "rocksdb/iterator.h"

namespace featureform {

namespace embedding {

std::vector<float> parse_rocks_value(const std::string& value) {
  auto proto = proto::Embedding();
  proto.ParseFromString(value);
  auto vals = proto.values();
  return std::vector<float>(vals.begin(), vals.end());
}

std::shared_ptr<EmbeddingStorage> EmbeddingStorage::load_or_create(std::string path, int dims) {
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::DB* db_ptr;
  rocksdb::Status status = rocksdb::DB::Open(options, path, &db_ptr);
  std::unique_ptr<rocksdb::DB> db(db_ptr);
  return std::unique_ptr<EmbeddingStorage>(new EmbeddingStorage(std::move(db), dims));
}

EmbeddingStorage::EmbeddingStorage(std::unique_ptr<rocksdb::DB> db, int dims)
    : db_(std::move(db)), dims_(dims) {
}

void EmbeddingStorage::set(std::string key, std::vector<float> val) {
  auto proto = proto::Embedding();
  *proto.mutable_values() = {val.begin(), val.end()};
  std::string serialized;
  proto.SerializeToString(&serialized);
  db_->Put(rocksdb::WriteOptions(), key, serialized);
}

std::vector<float> EmbeddingStorage::get(const std::string& key) const {
  std::string serialized;
  db_->Get(rocksdb::ReadOptions(), key, &serialized);
  return parse_rocks_value(serialized);
}

EmbeddingStorage::Iterator::Iterator(std::shared_ptr<EmbeddingStorage> storage) :
    first_{true}, iter_{storage->db_->NewIterator(rocksdb::ReadOptions())} {
    iter_->SeekToFirst();
}

bool EmbeddingStorage::Iterator::scan() {
    if (first_) {
        first_ = false;
    } else {
        iter_->Next();
    }
    return iter_->Valid();
}

std::string EmbeddingStorage::Iterator::key() {
  return iter_->key().ToString();
}

std::vector<float> EmbeddingStorage::Iterator::value() {
  return parse_rocks_value(iter_->value().ToString());
}

std::optional<std::string> EmbeddingStorage::Iterator::error() {
  if (!iter_->status().ok()) {
    return std::make_optional(iter_->status().ToString());
  }
  return std::nullopt;
}

}
}
