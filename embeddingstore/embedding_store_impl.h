/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#include "embedding_store.h"

namespace featureform {

namespace embeddings {

template <typename K, typename V>
void EmbeddingStore<K, V>::set(K key, V val) {
  this->data_[key] = val;
}

template <typename K, typename V>
const V& EmbeddingStore<K, V>::get(const K& key) const {
  return this->data_.at(key);
}
}
}
