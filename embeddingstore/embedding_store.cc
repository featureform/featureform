/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#include "embedding_store.h"

namespace featureform {

namespace embedding {

EmbeddingStore::EmbeddingStore(int dims)
    : dims_(dims), data_(), idx_(nullptr) {}

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
