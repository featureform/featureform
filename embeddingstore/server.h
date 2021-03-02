/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#pragma once

#include <memory>

#include "embeddingstore/controller.h"
#include "embeddingstore/embedding_store.grpc.pb.h"

namespace featureform {

namespace embedding {

class EmbeddingStoreService final : public proto::EmbeddingStore::Service {
 public:
  EmbeddingStoreService(std::unique_ptr<Controller> controller)
      : controller_(std::move(controller)){};

  grpc::Status CreateStore(grpc::ServerContext* context,
                           const proto::CreateStoreRequest* request,
                           proto::CreateStoreResponse* resp) override;

  grpc::Status DeleteStore(grpc::ServerContext* context,
                           const proto::DeleteStoreRequest* request,
                           proto::DeleteStoreResponse* resp) override;

  grpc::Status Set(grpc::ServerContext* context,
                   const proto::SetRequest* request,
                   proto::SetResponse* resp) override;

  grpc::Status MultiSet(grpc::ServerContext* context,
                        grpc::ServerReader<proto::MultiSetRequest>* reader,
                        proto::MultiSetResponse* resp) override;

  grpc::Status Get(grpc::ServerContext* context,
                   const proto::GetRequest* request,
                   proto::GetResponse* resp) override;

  grpc::Status GetNeighbors(
      grpc::ServerContext* context, const proto::GetNeighborsRequest* request,
      grpc::ServerWriter<proto::Neighbor>* writer) override;

  grpc::Status Download(
      grpc::ServerContext* context, const proto::DownloadRequest* request,
      grpc::ServerWriter<proto::DownloadResponse>* writer) override;

  grpc::Status Upload(grpc::ServerContext* context,
                      grpc::ServerReader<proto::UploadRequest>* reader,
                      proto::UploadResponse* resp) override;

 private:
  std::unique_ptr<Controller> controller_;
};

}  // namespace embedding
}  // namespace featureform

void RunServer();
