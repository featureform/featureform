/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#pragma once

#include <memory>
#include <optional>
#include <unordered_map>

#include "embeddingstore/embedding_store.grpc.pb.h"
#include "index.h"
#include "storage.h"

namespace featureform {

namespace embedding {

class EmbeddingStore {
 public:
  static std::unique_ptr<EmbeddingStore> load_or_create(std::string path,
                                                        std::string name,
                                                        int dims);
  static std::unique_ptr<EmbeddingStore> load_or_create_with_index(
      std::string path, std::string name, int dims);
  void set(std::string key, std::vector<float> value);
  const std::vector<float> get(const std::string& key) const;
  const bool check_exists(const std::string& key) const;
  std::shared_ptr<const ANNIndex> get_or_create_index();
  std::shared_ptr<const ANNIndex> get_index() const;
  void seed_index();
  std::vector<proto::Neighbor> get_neighbors(const std::string& key,
                                             size_t num) const;
  std::string get_path() const;
  std::string get_name() const;
  void set_name(std::string name);
  int get_dimensions() const;
  void import_proto(const std::string& filepath);
  std::string save(bool save_index);
  void close();
  void erase();

 private:
  EmbeddingStore(std::unique_ptr<EmbeddingStorage> storage, std::string path,
                 std::string name, int dims);
  std::unique_ptr<EmbeddingStorage> storage_;
  std::string path_;
  std::string name_;
  int dims_;
  std::shared_ptr<ANNIndex> idx_;
};

}  // namespace embedding
}  // namespace featureform
