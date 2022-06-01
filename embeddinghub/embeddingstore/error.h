/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#pragma once

#include <memory>

#include "rocksdb/status.h"

namespace featureform {

namespace embedding {

class ErrorBase {
 public:
  virtual ~ErrorBase(){};
  virtual std::string to_string() const = 0;
  virtual std::string type() const = 0;
};

class RocksDBError : public ErrorBase {
 public:
  static std::unique_ptr<RocksDBError> parse_optional(
      const rocksdb::Status status) {
    if (status.ok()) {
      return nullptr;
    }
    return std::make_unique<RocksDBError>(status);
  }

  RocksDBError(const rocksdb::Status status) : status_{status} {}

  std::string to_string() const override { return status_.ToString(); }

  std::string type() const override { return "RocksDBError"; }

 private:
  rocksdb::Status status_;
};

using Error = std::unique_ptr<ErrorBase>;

}  // namespace embedding
}  // namespace featureform
