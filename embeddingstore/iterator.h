/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#pragma once

#include <memory>
#include <optional>

#include "rocksdb/db.h"
#include "serializer.h"

namespace featureform {

namespace embedding {

class Iterator {
 public:
  Iterator(std::shared_ptr<rocksdb::DB> db);
  virtual ~Iterator();
  bool scan();
  std::string key();
  std::vector<float> value();
  std::optional<std::string> error();

 private:
  ProtoSerializer serializer_;
  bool first_;
  std::shared_ptr<rocksdb::DB> db_;
  const rocksdb::Snapshot* snapshot_;
  std::unique_ptr<rocksdb::Iterator> iter_;
};

}  // namespace embedding
}  // namespace featureform
