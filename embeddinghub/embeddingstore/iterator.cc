/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#include "iterator.h"

#include "embeddingstore/embedding_store.grpc.pb.h"
#include "rocksdb/db.h"
#include "rocksdb/iterator.h"

namespace featureform {

namespace embedding {

rocksdb::ReadOptions snapshot_read_opts(const rocksdb::Snapshot* snapshot) {
  auto options = rocksdb::ReadOptions();
  options.snapshot = snapshot;
  return options;
}

Iterator::Iterator(std::shared_ptr<rocksdb::DB> db)
    : serializer_{},
      first_{true},
      db_{db},
      snapshot_{db_->GetSnapshot()},
      iter_{db_->NewIterator(snapshot_read_opts(snapshot_))} {
  iter_->SeekToFirst();
}

Iterator::~Iterator() { db_->ReleaseSnapshot(snapshot_); }

bool Iterator::scan() {
  if (first_) {
    first_ = false;
  } else {
    iter_->Next();
  }
  return iter_->Valid();
}

std::string Iterator::key() { return iter_->key().ToString(); }

std::vector<float> Iterator::value() {
  return serializer_.deserialize(iter_->value().ToString());
}

std::optional<std::string> Iterator::error() {
  if (!iter_->status().ok()) {
    return std::make_optional(iter_->status().ToString());
  }
  return std::nullopt;
}

}  // namespace embedding
}  // namespace featureform
