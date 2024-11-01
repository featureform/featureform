/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#include "index.h"

namespace featureform {
namespace embedding {

ANNIndex::ANNIndex(size_t dims, size_t init_cap)
    : capacity_{init_cap},
      space_impl_(std::unique_ptr<hnswlib::SpaceInterface<float>>(
          new hnswlib::L2Space(dims))),
      nn_impl_(std::unique_ptr<hnswlib::HierarchicalNSW<float>>(
          new hnswlib::HierarchicalNSW<float>(space_impl_.get(), capacity_))),
      key_to_label_(),
      label_to_key_(),
      next_label_(0) {}

void ANNIndex::set(std::string key, std::vector<float> value) {
  auto label_iter = key_to_label_.find(key);
  auto is_new_key = label_iter == key_to_label_.end();
  hnswlib::labeltype label;
  if (is_new_key) {
    label = next_label_;
    next_label_++;
    label_to_key_[label] = key;
    key_to_label_[key] = label;
    if (next_label_ == capacity_) {
      capacity_ *= 2;
      nn_impl_->resizeIndex(capacity_);
    }
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
}  // namespace embedding
}  // namespace featureform
