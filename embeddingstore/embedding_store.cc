/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#include "embedding_store.h"

#include <iostream>
#include <cstdlib>
#include <memory>

#include "rocksdb/db.h"

namespace featureform {

namespace embedding {

std::unique_ptr<EmbeddingStore> EmbeddingStore::load_or_create(std::string path, int dims) {
  auto storage = EmbeddingStorage::load_or_create(path, dims);
  return std::unique_ptr<EmbeddingStore>(new EmbeddingStore(std::move(storage), dims));
}

EmbeddingStore::EmbeddingStore(std::unique_ptr<EmbeddingStorage> storage, int dims)
    :storage_(std::move(storage)), dims_(dims), data_(), idx_(nullptr) {
}

void EmbeddingStore::set(std::string key, std::vector<float> val) {
  if (idx_ != nullptr) {
    idx_->set(key, val);
  }
  data_[key] = val;
}

const std::vector<float>& EmbeddingStore::get(const std::string& key) const {
  return data_.at(key);
}

std::shared_ptr<const ANNIndex> EmbeddingStore::create_ann_index() {
  if (idx_ != nullptr) {
    return idx_;
  }
  idx_ = std::make_shared<ANNIndex>(dims_);
  for (const std::pair<std::string, std::vector<float>> row : data_) {
    auto key = row.first;
    auto val = row.second;
    idx_->set(key, val);
  }
  return idx_;
}

std::shared_ptr<const ANNIndex> EmbeddingStore::get_ann_index() const {
  return idx_;
}
}
}
