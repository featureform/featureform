/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#pragma once

#include <memory>

#include "embeddingstore/embedding_store.grpc.pb.h"
#include "embeddingstore/embedding_store.h"

namespace featureform {

namespace embedding {

class EmbeddingStoreService final : public proto::EmbeddingStore::Service {
 public:
  EmbeddingStoreService()
      : store_(std::unique_ptr<EmbeddingStore<std::string, std::vector<float>>>(
            new EmbeddingStore<std::string, std::vector<float>>)){};
  grpc::Status Set(grpc::ServerContext* context,
                   const proto::SetRequest* request,
                   proto::SetResponse* resp) override;
  grpc::Status Get(grpc::ServerContext* context,
                   const proto::GetRequest* request,
                   proto::GetResponse* resp) override;

 private:
  std::unique_ptr<EmbeddingStore<std::string, std::vector<float>>> store_;
};
}
}

void RunServer();
