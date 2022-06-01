/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#include "version.h"

#include <cstdlib>
#include <iostream>
#include <memory>

#include "error.h"
#include "iterator.h"
#include "rocksdb/db.h"

namespace featureform {

namespace embedding {

std::shared_ptr<Version> Version::load_or_create(std::string path,
                                                 std::string space,
                                                 std::string name, int dims,
                                                 bool immutable) {
  auto storage = EmbeddingStorage::load_or_create(path, dims);
  return std::unique_ptr<Version>(
      new Version(std::move(storage), space, name, dims, immutable));
}

Version::Version(std::shared_ptr<EmbeddingStorage> storage, std::string space,
                 std::string name, int dims, bool immutable)
    : storage_{std::move(storage)},
      space_{space},
      name_{name},
      dims_{dims},
      immutable_{immutable},
      idx_{nullptr} {}

std::string Version::space() const { return space_; }

std::string Version::name() const { return name_; }

int Version::dims() const { return dims_; }

bool Version::immutable() const { return immutable_; }

void Version::make_immutable() { immutable_ = true; }

Error Version::set(std::string key, std::vector<float> val) {
  if (immutable_) {
    return UpdateImmutableVersionError::create(name_);
  }
  storage_->set(key, val);
  if (idx_ != nullptr) {
    idx_->set(key, val);
  }
  return nullptr;
}

std::vector<float> Version::get(const std::string& key) const {
  return storage_->get(key);
}

Iterator Version::iterator() const { return storage_->iterator(); }

std::shared_ptr<const ANNIndex> Version::create_ann_index() {
  if (idx_ != nullptr) {
    return idx_;
  }
  idx_ = std::make_shared<ANNIndex>(dims_);
  auto iter = storage_->iterator();
  while (iter.scan()) {
    idx_->set(iter.key(), iter.value());
  }
  return idx_;
}

std::shared_ptr<const ANNIndex> Version::get_ann_index() const { return idx_; }
}  // namespace embedding
}  // namespace featureform
