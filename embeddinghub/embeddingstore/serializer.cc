/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#include "serializer.h"

#include "embeddingstore/embedding_store.grpc.pb.h"

namespace featureform {

namespace embedding {

std::vector<float> ProtoSerializer::deserialize(
    const std::string& serialized) const {
  auto proto = proto::Embedding();
  proto.ParseFromString(serialized);
  auto vals = proto.values();
  return std::vector<float>(vals.begin(), vals.end());
}

std::string ProtoSerializer::serialize(std::vector<float> vals) const {
  auto proto = proto::Embedding();
  *proto.mutable_values() = {vals.begin(), vals.end()};
  std::string serialized;
  proto.SerializeToString(&serialized);
  return serialized;
}

}  // namespace embedding
}  // namespace featureform
