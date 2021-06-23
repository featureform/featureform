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
#include "storage.h"

namespace featureform {

namespace embedding {

class Space {
 public:
  static std::shared_ptr<Space> load_or_create(std::string path,
                                               std::string name, int dims,
                                               bool immutable = false);
  Error set(std::string key, std::vector<float> value);
  std::string name() const;
  int dims() const;
  bool immutable() const;
  void make_immutable();
  std::vector<float> get(const std::string& key) const;
  std::shared_ptr<const ANNIndex> create_ann_index();
  std::shared_ptr<const ANNIndex> get_ann_index() const;

 private:
  Space(std::shared_ptr<EmbeddingStorage> storage, std::string name, int dims,
        bool immutable);
  std::shared_ptr<EmbeddingStorage> storage_;
  std::string name_;
  int dims_;
  bool immutable_;
  std::shared_ptr<ANNIndex> idx_;
};

class UpdateImmutableSpaceError : public ErrorBase {
 public:
  static Error create(std::string space) {
    return std::make_unique<UpdateImmutableSpaceError>(space);
  }
  UpdateImmutableSpaceError(std::string space) : space_{space} {};
  std::string to_string() const override {
    std::ostringstream stream;
    stream << "Cannot update " << space_ << ": space is immutable.";
    return stream.str();
  };
  std::string type() const override { return "UpdateImmutableSpaceError"; }

 private:
  std::string space_;
};
}  // namespace embedding
}  // namespace featureform
