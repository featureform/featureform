/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#pragma once

#include <memory>
#include <vector>

#include "hnswlib/hnswlib.h"

namespace featureform {

namespace embedding {

template <typename K, typename V>
class ANNIndex {
 public:
  ANNIndex(int dims);
  void set(K key, V value);
  std::vector<K> approx_nearest(V value, size_t num);

 private:
  std::unique_ptr<hnswlib::SpaceInterface<float>> space_impl_;
  std::unique_ptr<hnswlib::HierarchicalNSW<float>> nn_impl_;
  std::unordered_map<K, hnswlib::labeltype> key_to_label_;
  std::unordered_map<hnswlib::labeltype, K> label_to_key_;
  hnswlib::labeltype next_label_;
};
}
}

#include "index_impl.h"
