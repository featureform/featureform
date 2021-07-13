/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#pragma once

#include <memory>

#include "embeddingstore/embedding_store.grpc.pb.h"
#include "embeddingstore/embedding_store.h"

namespace featureform {

namespace embedding {

class EmbeddingHubService final : public proto::EmbeddingHub::Service {
 public:
  EmbeddingHubService(std::shared_ptr<EmbeddingHub> store)
      : store_(std::move(store)), mtx_{} {};

  grpc::Status CreateSpace(grpc::ServerContext* context,
                           const proto::CreateSpaceRequest* request,
                           proto::CreateSpaceResponse* resp) override;

  grpc::Status FreezeSpace(grpc::ServerContext* context,
                           const proto::FreezeSpaceRequest* request,
                           proto::FreezeSpaceResponse* resp) override;

  grpc::Status MultiSet(grpc::ServerContext* context,
                        grpc::ServerReader<proto::MultiSetRequest>* reader,
                        proto::MultiSetResponse* resp) override;

  grpc::Status MultiGet(
      grpc::ServerContext* context,
      grpc::ServerReaderWriter<proto::MultiGetResponse, proto::MultiGetRequest>*
          stream) override;

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
  std::optional<std::shared_ptr<Version>> GetVersion(
      const std::string& space, const std::string& version);
  std::shared_ptr<EmbeddingHub> store_;
  std::mutex mtx_;
};
}  // namespace embedding
}  // namespace featureform

void RunServer();
