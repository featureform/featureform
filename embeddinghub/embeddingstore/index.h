/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#pragma once

#include <memory>
#include <vector>

#include "hnswlib/hnswlib.h"

namespace featureform {

namespace embedding {

class ANNIndex {
 public:
  ANNIndex(size_t dims, size_t init_cap = 128);
  void set(std::string key, std::vector<float> value);
  std::vector<std::string> approx_nearest(std::vector<float> value,
                                          size_t num) const;

 private:
  size_t capacity_;
  std::unique_ptr<hnswlib::SpaceInterface<float>> space_impl_;
  std::unique_ptr<hnswlib::HierarchicalNSW<float>> nn_impl_;
  std::unordered_map<std::string, hnswlib::labeltype> key_to_label_;
  std::unordered_map<hnswlib::labeltype, std::string> label_to_key_;
  hnswlib::labeltype next_label_;
};
}  // namespace embedding
}  // namespace featureform
