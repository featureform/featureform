/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#pragma once

#include <memory>
#include <optional>
#include <unordered_map>

#include "index.h"
#include "storage.h"

namespace featureform {

namespace embedding {

class EmbeddingStore {
 public:
  static std::unique_ptr<EmbeddingStore> load_or_create(std::string path, int dims);
  void set(std::string key, std::vector<float> value);
  std::vector<float> get(const std::string& key) const;
  std::shared_ptr<const ANNIndex> create_ann_index();
  std::shared_ptr<const ANNIndex> get_ann_index() const;

 private:
  EmbeddingStore(std::unique_ptr<EmbeddingStorage> storage, int dims);
  std::shared_ptr<EmbeddingStorage> storage_;
  int dims_;
  std::shared_ptr<ANNIndex> idx_;
};
}
}
