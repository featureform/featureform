/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#include "storage.h"

#include <glog/logging.h>

#include "embeddingstore/embedding_store.grpc.pb.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/backupable_db.h"

namespace featureform {

namespace embedding {

std::unique_ptr<EmbeddingStorage> EmbeddingStorage::load_or_create(
    std::string path, int dims) {
  DLOG(INFO) << "loading storage from path: " << path << " dims: " << dims;
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::DB* db_ptr;
  rocksdb::Status status = rocksdb::DB::Open(options, path, &db_ptr);
  std::unique_ptr<rocksdb::DB> db(db_ptr);
  return std::unique_ptr<EmbeddingStorage>(
      new EmbeddingStorage(std::move(db), path, dims));
}

EmbeddingStorage::EmbeddingStorage(std::unique_ptr<rocksdb::DB> db,
                                   std::string path, int dims)
    : db_(std::move(db)), path_(path), dims_(dims) {}

void EmbeddingStorage::set(std::string key, std::vector<float> val) {
  auto proto = proto::Embedding();
  *proto.mutable_values() = {val.begin(), val.end()};
  std::string serialized;
  proto.SerializeToString(&serialized);
  db_->Put(rocksdb::WriteOptions(), key, serialized);
}

std::vector<float> EmbeddingStorage::get(const std::string& key) const {
  std::string serialized;
  rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), key, &serialized);
  auto proto = proto::Embedding();
  proto.ParseFromString(serialized);
  auto vals = proto.values();
  return std::vector<float>(vals.begin(), vals.end());
}

std::unordered_map<std::string, std::vector<float>> EmbeddingStorage::get_all()
    const {
  std::string numstr;
  db_->GetProperty("rocksdb.estimate-num-keys", &numstr);
  std::unordered_map<std::string, std::vector<float>> out;
  out.reserve(std::stoi(numstr));
  rocksdb::Iterator* it = db_->NewIterator(rocksdb::ReadOptions());
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    auto proto = proto::Embedding();
    proto.ParseFromString(it->value().ToString());
    auto vals = proto.values();
    auto embedding = std::vector<float>(vals.begin(), vals.end());
    out[it->key().ToString()] = embedding;
  }
  return out;
}

void EmbeddingStorage::backup_to(const std::string& dst) const {
  rocksdb::BackupEngine* backup_engine;
  auto s = rocksdb::BackupEngine::Open(rocksdb::Env::Default(),
                                       rocksdb::BackupableDBOptions(dst),
                                       &backup_engine);
  assert(s.ok());
  s = backup_engine->CreateNewBackup(db_.get());
  assert(s.ok());
  delete backup_engine;
}

void EmbeddingStorage::close() {
  db_->Close();
  db_.reset();
}

void EmbeddingStorage::erase() {
  db_->Close();
  delete db_.get();
  rocksdb::Options options;
  rocksdb::DestroyDB(path_, options);
}

}  // namespace embedding
}  // namespace featureform
