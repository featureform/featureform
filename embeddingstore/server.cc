/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#include "server.h"

#include <glog/logging.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include <iostream>
#include <memory>
#include <string>

using ::featureform::embedding::proto::CreateStoreRequest;
using ::featureform::embedding::proto::CreateStoreResponse;
using ::featureform::embedding::proto::DeleteStoreRequest;
using ::featureform::embedding::proto::DeleteStoreResponse;
using ::featureform::embedding::proto::Embedding;
using ::featureform::embedding::proto::GetNeighborsRequest;
using ::featureform::embedding::proto::GetRequest;
using ::featureform::embedding::proto::GetResponse;
using ::featureform::embedding::proto::MultiSetRequest;
using ::featureform::embedding::proto::MultiSetResponse;
using ::featureform::embedding::proto::Neighbor;
using ::featureform::embedding::proto::SetRequest;
using ::featureform::embedding::proto::SetResponse;
using ::grpc::Server;
using ::grpc::ServerBuilder;
using ::grpc::ServerContext;
using ::grpc::ServerReader;
using ::grpc::ServerWriter;
using ::grpc::Status;

namespace featureform {

namespace embedding {

std::vector<float> copy_embedding_to_vector(const Embedding& embedding) {
  auto vals = embedding.values();
  return std::vector<float>(vals.cbegin(), vals.cend());
}

grpc::Status EmbeddingStoreService::CreateStore(
    ServerContext* context, const CreateStoreRequest* request,
    CreateStoreResponse* resp) {
  DLOG(INFO) << "create store: " << request->store_name()
             << " dimensions: " << request->dimensions();
  controller_->create_store(request->store_name(), request->dimensions());
  return Status::OK;
}

grpc::Status EmbeddingStoreService::DeleteStore(
    ServerContext* context, const DeleteStoreRequest* request,
    DeleteStoreResponse* resp) {
  DLOG(INFO) << "delete store: " << request->store_name();
  controller_->delete_store(request->store_name());
  return Status::OK;
}

grpc::Status EmbeddingStoreService::Get(ServerContext* context,
                                        const GetRequest* request,
                                        GetResponse* resp) {
  DLOG(INFO) << "get store: " << request->store_name()
             << " key: " << request->key();
  auto store = controller_->get_store(request->store_name());
  auto embedding = store->get(request->key());
  std::unique_ptr<Embedding> proto_embedding(new Embedding());
  *proto_embedding->mutable_values() = {embedding.begin(), embedding.end()};
  resp->set_allocated_embedding(proto_embedding.release());
  return Status::OK;
}

grpc::Status EmbeddingStoreService::Set(ServerContext* context,
                                        const SetRequest* request,
                                        SetResponse* resp) {
  auto vec = copy_embedding_to_vector(request->embedding());
  auto store = controller_->get_store(request->store_name());
  store->set(request->key(), vec);
  return Status::OK;
}

grpc::Status EmbeddingStoreService::MultiSet(
    ServerContext* context, ServerReader<MultiSetRequest>* reader,
    MultiSetResponse* resp) {
  std::shared_ptr<EmbeddingStore> store;
  MultiSetRequest request;
  while (reader->Read(&request)) {
    store = controller_->get_store(request.store_name());
    auto vec = copy_embedding_to_vector(request.embedding());
    store->set(request.key(), vec);
  }
  return Status::OK;
}

grpc::Status EmbeddingStoreService::GetNeighbors(
    ServerContext* context, const GetNeighborsRequest* request,
    ServerWriter<Neighbor>* writer) {
  DLOG(INFO) << "loading " << request->number()
             << " neighbors for key: " << request->key() << " ...";
  auto store = controller_->get_store(request->store_name());
  auto neighbors = store->get_neighbors(request->key(), request->number());
  DLOG(INFO) << "loaded neighbors";
  for (const Neighbor& n : neighbors) {
    writer->Write(n);
  }
  return Status::OK;
}

}  // namespace embedding
}  // namespace featureform

using featureform::embedding::Controller;
using featureform::embedding::EmbeddingStore;
using featureform::embedding::EmbeddingStoreService;

void RunServer() {
  std::string server_address("0.0.0.0:50051");
  auto controller = Controller::load_or_create("embedding_store.dat");
  auto service = EmbeddingStoreService(std::move(controller));

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
  LOG(INFO) << "Server listening on " << server_address;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
  LOG(INFO) << "Server returning";
}
