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
  EmbeddingStoreService(std::unique_ptr<EmbeddingStore> store) : store_(std::move(store)){};

  grpc::Status MultiSet(grpc::ServerContext* context,
                        grpc::ServerReader<proto::MultiSetRequest>* reader,
                        proto::MultiSetResponse* resp) override;

  grpc::Status Set(grpc::ServerContext* context,
                   const proto::SetRequest* request,
                   proto::SetResponse* resp) override;

  grpc::Status Get(grpc::ServerContext* context,
                   const proto::GetRequest* request,
                   proto::GetResponse* resp) override;

  grpc::Status NearestNeighbor(grpc::ServerContext* context,
							   const proto::NearestNeighborRequest* request,
							   proto::NearestNeighborResponse* resp) override;

 private:
  std::unique_ptr<EmbeddingStore> store_;
};
}
}

void RunServer();
