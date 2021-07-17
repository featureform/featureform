/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#include "server.h"

int main(int argc, char** argv) {
  auto address = argc > 1 ? argv[1] : "0.0.0.0:74622";
  RunServer(address);

  return 0;
}
