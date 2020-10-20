/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#include "server.h"

#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

using ::grpc::Server;
using ::grpc::ServerBuilder;
using ::grpc::ServerContext;
using ::grpc::Status;

namespace featureform {

namespace embedding {

grpc::Status EmbeddingStoreService::Get(ServerContext* context,
                                        const proto::GetRequest* request,
                                        proto::GetResponse* resp) {
  auto embedding = store_->get(request->key());
  std::unique_ptr<proto::Embedding> proto_embedding(new proto::Embedding());
  *proto_embedding->mutable_values() = {embedding.begin(), embedding.end()};
  resp->set_allocated_embedding(proto_embedding.release());
  return Status::OK;
}

grpc::Status EmbeddingStoreService::Set(ServerContext* context,
                                        const proto::SetRequest* request,
                                        proto::SetResponse* resp) {
  auto vals = request->embedding().values();
  auto embedding = std::vector<float>(vals.cbegin(), vals.cend());
  store_->set(request->key(), embedding);
  return Status::OK;
}
}
}

void RunServer() {
  std::string server_address("0.0.0.0:50051");
  featureform::embedding::EmbeddingStoreService service;

  grpc::EnableDefaultHealthCheckService(true);
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}
