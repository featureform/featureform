/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#pragma once

namespace featureform {

namespace embedding {

class Error {
 public:
  virtual std::string to_string() = 0;
  virtual std::string type() = 0;
};

class RocksDBError : public Error {
 public:
  static std::optional<RocksDBError> parse_optional(
      const rocksdb::Status status) {
    if (status.ok()) {
      return std::nullopt;
    }
    return RocksDBError(status);
  }

  RocksDBError(const rocksdb::Status status) : status_{status} {}

  std::string to_string() const { return status_.ToString(); }

  std::string type() const { return "RocksDBError"; }

 private:
  rocksdb::Status status_;
}

}  // namespace embedding
}  // namespace featureform
