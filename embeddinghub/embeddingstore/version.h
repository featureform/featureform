/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#pragma once

#include <memory>
#include <optional>
#include <sstream>
#include <unordered_map>

#include "error.h"
#include "index.h"
#include "iterator.h"
#include "storage.h"

namespace featureform {

namespace embedding {

class Version {
 public:
  static std::shared_ptr<Version> load_or_create(std::string path,
                                                 std::string space,
                                                 std::string name, int dims,
                                                 bool immutable = false);
  Error set(std::string key, std::vector<float> value);
  std::string space() const;
  std::string name() const;
  int dims() const;
  bool immutable() const;
  void make_immutable();
  std::vector<float> get(const std::string& key) const;
  Iterator iterator() const;
  std::shared_ptr<const ANNIndex> create_ann_index();
  std::shared_ptr<const ANNIndex> get_ann_index() const;

 private:
  Version(std::shared_ptr<EmbeddingStorage> storage, std::string space,
          std::string name, int dims, bool immutable);
  std::shared_ptr<EmbeddingStorage> storage_;
  std::string space_;
  std::string name_;
  int dims_;
  bool immutable_;
  std::shared_ptr<ANNIndex> idx_;
};

class UpdateImmutableVersionError : public ErrorBase {
 public:
  static Error create(std::string version) {
    return std::make_unique<UpdateImmutableVersionError>(version);
  }
  UpdateImmutableVersionError(std::string version) : version_{version} {};
  std::string to_string() const override {
    std::ostringstream stream;
    stream << "Cannot update " << version_ << ": version is immutable.";
    return stream.str();
  };
  std::string type() const override { return "UpdateImmutableVersionError"; }

 private:
  std::string version_;
};
}  // namespace embedding
}  // namespace featureform
