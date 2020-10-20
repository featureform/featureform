/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#pragma once

#include <unordered_map>

namespace featureform {

namespace embedding {

template <typename K, typename V>
class EmbeddingStore {
 public:
  EmbeddingStore() = default;
  void set(K key, V value);
  const V& get(const K& key) const;

 private:
  ::std::unordered_map<K, V> data_;
};
}
}

#include "embedding_store_impl.h"
