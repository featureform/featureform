/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#include "embedding_store.h"

#include <glog/logging.h>

#include <cstdlib>
#include <iostream>
#include <memory>

#include "rocksdb/db.h"

using ::featureform::embedding::proto::Neighbor;

namespace featureform {

namespace embedding {

std::unique_ptr<EmbeddingStore> EmbeddingStore::load_or_create(std::string path,
                                                               std::string name,
                                                               int dims) {
  auto storage = EmbeddingStorage::load_or_create(path, dims);
  return std::unique_ptr<EmbeddingStore>(
      new EmbeddingStore(std::move(storage), path, name, dims));
}

std::unique_ptr<EmbeddingStore> EmbeddingStore::load_or_create_with_index(
    std::string path, std::string name, int dims) {
  auto storage = EmbeddingStorage::load_or_create(path, dims);
  auto store = std::unique_ptr<EmbeddingStore>(
      new EmbeddingStore(std::move(storage), path, name, dims));
  store->get_or_create_index();
  return store;
}

EmbeddingStore::EmbeddingStore(std::unique_ptr<EmbeddingStorage> storage,
                               std::string path, std::string name, int dims)
    : storage_(std::move(storage)),
      path_(path),
      name_(name),
      dims_(dims),
      idx_(nullptr) {}

std::string EmbeddingStore::get_path() const { return path_; }
std::string EmbeddingStore::get_name() const { return name_; }
void EmbeddingStore::set_name(std::string name) { name_ = name; }
int EmbeddingStore::get_dimensions() const { return dims_; }

void EmbeddingStore::import_proto(const std::string& filepath) {
  storage_->proto_in(filepath);
  if (idx_ != nullptr) {
    seed_index();
  }
}

std::string EmbeddingStore::save(bool save_index) {
  std::string path = path_ + "_out.proto";
  storage_->proto_out(path);
  return path;
}

void EmbeddingStore::close() {
  DLOG(INFO) << "store: " << name_ << " storage closing...";
  storage_->close();
  storage_.reset();
  DLOG(INFO) << "store: " << name_ << " storage closed";
  if (idx_ != nullptr) {
    DLOG(INFO) << "store: " << name_ << " closing index...";
    idx_.reset();
    DLOG(INFO) << "store: " << name_ << " index closed";
  }
}

void EmbeddingStore::erase() {
  storage_->erase();
  storage_.reset();
  if (idx_ != nullptr) {
    idx_->erase();
    idx_.reset();
  }
}

void EmbeddingStore::set(std::string key, std::vector<float> val) {
  if (idx_ != nullptr) {
    idx_->set(key, val);
  }
  storage_->set(key, val);
}

const std::vector<float> EmbeddingStore::get(const std::string& key) const {
  return storage_->get(key);
}

// Create hnsw index for the class instance
std::shared_ptr<const ANNIndex> EmbeddingStore::get_or_create_index() {
  if (idx_ != nullptr) {
    return idx_;
  }
  idx_ = std::make_shared<ANNIndex>(dims_);
  seed_index();
  return idx_;
}

std::shared_ptr<const ANNIndex> EmbeddingStore::get_index() const {
  return idx_;
}

void EmbeddingStore::seed_index() {
  auto data = storage_->get_all();
  for (const std::pair<std::string, std::vector<float>> row : data) {
    auto key = row.first;
    auto val = row.second;
    idx_->set(key, val);
  }
}

std::vector<Neighbor> EmbeddingStore::get_neighbors(const std::string& key,
                                                    size_t num) const {
  auto embedding = get(key);
  return idx_->get_neighbors(embedding, num, key);
}

}  // namespace embedding
}  // namespace featureform
