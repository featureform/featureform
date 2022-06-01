/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#pragma once

#include <string>
#include <vector>

namespace featureform {

namespace embedding {

class ProtoSerializer {
 public:
  std::vector<float> deserialize(const std::string& serialized) const;
  std::string serialize(std::vector<float> vals) const;
};
}  // namespace embedding
}  // namespace featureform
