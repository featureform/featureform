/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#pragma once

namespace featureform {

namespace embedding {

class Error {
 public:
  virtual std::string to_string() = 0;
  virtual std::string type() = 0;
};

}  // namespace embedding
}  // namespace featureform
