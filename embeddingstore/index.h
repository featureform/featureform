/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#pragma once

#include <memory>
#include <vector>

#include "embeddingstore/embedding_store.grpc.pb.h"
#include "hnswlib/hnswlib.h"

namespace featureform {

namespace embedding {

class ANNIndex {
 public:
  ANNIndex(int dims);
  void set(std::string key, std::vector<float> value);

  std::vector<std::string> approx_nearest(std::vector<float> value,
                                          size_t num) const;
  std::vector<std::pair<std::string, float>> approx_nearest_pairs(
      std::vector<float> value, size_t num,
      const std::string& exclude_key) const;
  std::vector<proto::Neighbor> get_neighbors(
      std::vector<float> value, size_t num,
      const std::string& exclude_key) const;
  void erase();

 private:
  std::unique_ptr<hnswlib::SpaceInterface<float>> space_impl_;
  std::unique_ptr<hnswlib::HierarchicalNSW<float>> nn_impl_;
  std::unordered_map<std::string, hnswlib::labeltype> key_to_label_;
  std::unordered_map<hnswlib::labeltype, std::string> label_to_key_;
  hnswlib::labeltype next_label_;
};

}  // namespace embedding
}  // namespace featureform
