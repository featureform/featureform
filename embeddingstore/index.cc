/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#include "index.h"

using ::featureform::embedding::proto::Neighbor;

#include <glog/logging.h>

namespace featureform {
namespace embedding {

ANNIndex::ANNIndex(int dims)
    : space_impl_(std::unique_ptr<hnswlib::SpaceInterface<float>>(
          new hnswlib::L2Space(dims))),
      nn_impl_(std::unique_ptr<hnswlib::HierarchicalNSW<float>>(
          new hnswlib::HierarchicalNSW<float>(space_impl_.get(), 100))),
      key_to_label_(),
      label_to_key_(),
      next_label_(0) {}

// Adds key-value pair to the approximate nearest neighbor index.
// Keys are converted internally from string to hnsw label (index).
void ANNIndex::set(std::string key, std::vector<float> value) {
  auto label_iter = key_to_label_.find(key);
  auto is_new_key = label_iter == key_to_label_.end();
  hnswlib::labeltype label;
  if (is_new_key) {
    label = next_label_;
    next_label_++;
    label_to_key_[label] = key;
    key_to_label_[key] = label;
  } else {
    label = label_iter->second;
  }
  nn_impl_->addPoint(value.data(), label);
}

std::vector<std::string> ANNIndex::approx_nearest(std::vector<float> value,
                                                  size_t num) const {
  auto dist_label_pairs = nn_impl_->searchKnn(value.data(), num);
  auto nearest_keys = std::vector<std::string>(num);
  // hnswlib returns things in backwards order, so we have to
  // reverse it:
  // https://github.com/nmslib/hnswlib/issues/7
  for (int i = num - 1; i >= 0; i--) {
    const auto label = dist_label_pairs.top().second;
    nearest_keys[i] = label_to_key_.at(label);
    dist_label_pairs.pop();
  }
  return nearest_keys;
}

std::vector<std::pair<std::string, float>> ANNIndex::approx_nearest_pairs(
    std::vector<float> value, size_t num,
    const std::string& exclude_key) const {
  auto dist_label_pairs = nn_impl_->searchKnn(value.data(), num + 1);
  auto out = std::vector<std::pair<std::string, float>>(num);

  for (int i = num - 1; i >= 0; i--) {
    const auto label = dist_label_pairs.top().second;
    // Skip excluded key
    if (label_to_key_.at(label) == exclude_key) {
      dist_label_pairs.pop();
      i++;
      continue;
    }
    out[i] = std::pair<std::string, float>(label_to_key_.at(label),
                                           dist_label_pairs.top().first);
    dist_label_pairs.pop();
  }
  return out;
}

std::vector<Neighbor> ANNIndex::get_neighbors(
    std::vector<float> value, size_t num,
    const std::string& exclude_key) const {
  DLOG(INFO) << "getting neighbors...";
  auto dist_label_pairs = nn_impl_->searchKnn(value.data(), num + 1);
  auto out = std::vector<Neighbor>(num);

  for (int i = num - 1; i >= 0; i--) {
    const auto label = dist_label_pairs.top().second;
    // Skip excluded key
    if (label_to_key_.at(label) == exclude_key) {
      dist_label_pairs.pop();
      i++;
      continue;
    }
    Neighbor n;
    n.set_key(label_to_key_.at(label));
    n.set_distance(dist_label_pairs.top().first);
    out[i] = n;
    dist_label_pairs.pop();
  }
  DLOG(INFO) << "returning neighbors";
  return out;
}

void ANNIndex::erase() {}

}  // namespace embedding
}  // namespace featureform
