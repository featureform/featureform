/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#include "space.h"

#include <cstdlib>
#include <iostream>
#include <memory>

#include "error.h"
#include "rocksdb/db.h"

namespace featureform {

namespace embedding {

std::shared_ptr<Space> Space::load_or_create(std::string path, std::string name,
                                             int dims, bool immutable) {
  auto storage = EmbeddingStorage::load_or_create(path, dims);
  return std::unique_ptr<Space>(
      new Space(std::move(storage), name, dims, immutable));
}

Space::Space(std::shared_ptr<EmbeddingStorage> storage, std::string name,
             int dims, bool immutable)
    : storage_{std::move(storage)},
      name_{name},
      dims_{dims},
      immutable_{immutable},
      idx_{nullptr} {}

std::string Space::name() const { return name_; }

int Space::dims() const { return dims_; }

bool Space::immutable() const { return immutable_; }

void Space::make_immutable() { immutable_ = true; }

Error Space::set(std::string key, std::vector<float> val) {
  if (immutable_) {
    return UpdateImmutableSpaceError::create(name_);
  }
  storage_->set(key, val);
  if (idx_ != nullptr) {
    idx_->set(key, val);
  }
  return nullptr;
}

std::vector<float> Space::get(const std::string& key) const {
  return storage_->get(key);
}

std::shared_ptr<const ANNIndex> Space::create_ann_index() {
  if (idx_ != nullptr) {
    return idx_;
  }
  idx_ = std::make_shared<ANNIndex>(dims_);
  auto iter = EmbeddingStorage::Iterator(storage_);
  while (iter.scan()) {
    idx_->set(iter.key(), iter.value());
  }
  return idx_;
}

std::shared_ptr<const ANNIndex> Space::get_ann_index() const { return idx_; }
}  // namespace embedding
}  // namespace featureform
