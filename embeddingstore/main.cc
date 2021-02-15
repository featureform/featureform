/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#include <glog/logging.h>

#include "server.h"

int main(int argc, char** argv) {
  FLAGS_logbufsecs = 0;  // Enforces constant flushing to file to prevent missed
                         // log statements when debugging.
  google::SetLogDestination(google::GLOG_INFO, "./embeddings.log");
  google::InitGoogleLogging("embeddings");
  RunServer();
  return 0;
}
