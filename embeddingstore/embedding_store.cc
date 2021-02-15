/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#include "embedding_store.h"

#include <cstdlib>
#include <iostream>
#include <memory>

#include "rocksdb/db.h"

using ::featureform::embedding::proto::Neighbor;

namespace featureform {

namespace embedding {

std::unique_ptr<EmbeddingStore> EmbeddingStore::load_or_create(std::string path,
                                                               int dims) {
  auto storage = EmbeddingStorage::load_or_create(path, dims);
  return std::unique_ptr<EmbeddingStore>(
      new EmbeddingStore(std::move(storage), dims));
}

std::unique_ptr<EmbeddingStore> EmbeddingStore::load_or_create_with_index(
    std::string path, int dims) {
  auto storage = EmbeddingStorage::load_or_create(path, dims);
  auto store = std::unique_ptr<EmbeddingStore>(
      new EmbeddingStore(std::move(storage), dims));
  store->get_or_create_index();
  return store;
}

EmbeddingStore::EmbeddingStore(std::unique_ptr<EmbeddingStorage> storage,
                               int dims)
    : storage_(std::move(storage)), dims_(dims), idx_(nullptr) {}

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
  auto data = storage_->get_all();
  for (const std::pair<std::string, std::vector<float>> row : data) {
    auto key = row.first;
    auto val = row.second;
    idx_->set(key, val);
  }
  return idx_;
}

std::shared_ptr<const ANNIndex> EmbeddingStore::get_index() const {
  return idx_;
}

std::vector<Neighbor> EmbeddingStore::get_neighbors(const std::string& key,
                                                    size_t num) const {
  auto embedding = get(key);
  return idx_->get_neighbors(embedding, num, key);
}

}  // namespace embedding
}  // namespace featureform
