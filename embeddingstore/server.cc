/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#include "server.h"

#include <algorithm>
#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

using ::grpc::Server;
using ::grpc::ServerBuilder;
using ::grpc::ServerContext;
using ::grpc::ServerReader;
using ::grpc::ServerReaderWriter;
using ::grpc::Status;
using ::featureform::embedding::proto::Embedding;
using ::featureform::embedding::proto::GetRequest;
using ::featureform::embedding::proto::GetResponse;
using ::featureform::embedding::proto::SetRequest;
using ::featureform::embedding::proto::SetResponse;
using ::featureform::embedding::proto::MultiSetRequest;
using ::featureform::embedding::proto::MultiSetResponse;
using ::featureform::embedding::proto::MultiGetRequest;
using ::featureform::embedding::proto::MultiGetResponse;
using ::featureform::embedding::proto::NearestNeighborRequest;
using ::featureform::embedding::proto::NearestNeighborResponse;

namespace featureform {

namespace embedding {

std::vector<float> copy_embedding_to_vector(const Embedding& embedding) {
  auto vals = embedding.values();
  return std::vector<float>(vals.cbegin(), vals.cend());
}

template<typename T>
bool remove_uniq_value(std::vector<T>& vec, T val) {
    auto pos = std::find(vec.begin(), vec.end(), val);
    if (pos != vec.end()) {
        vec.erase(pos);
        return true;
    }
    return false;
}

grpc::Status EmbeddingStoreService::Get(ServerContext* context,
                                        const GetRequest* request,
                                        GetResponse* resp) {
  auto embedding = store_->create_space(request->space(), 3)->get(request->key());
  std::unique_ptr<Embedding> proto_embedding(new Embedding());
  *proto_embedding->mutable_values() = {embedding.begin(), embedding.end()};
  resp->set_allocated_embedding(proto_embedding.release());
  return Status::OK;
}

grpc::Status EmbeddingStoreService::Set(ServerContext* context,
                                        const SetRequest* request,
                                        SetResponse* resp) {
  auto vec = copy_embedding_to_vector(request->embedding());
  store_->create_space(request->space(), 3)->set(request->key(), vec);
  return Status::OK;
}

grpc::Status EmbeddingStoreService::MultiSet(
    ServerContext* context, ServerReader<MultiSetRequest>* reader,
    MultiSetResponse* resp) {
    MultiSetRequest request;
  while (reader->Read(&request)) {
    auto vec = copy_embedding_to_vector(request.embedding());
    store_->create_space(request.space(), 3)->set(request.key(), vec);
  }
  return Status::OK;
}

grpc::Status EmbeddingStoreService::MultiGet(ServerContext* context,
                                    ServerReaderWriter<MultiGetResponse, MultiGetRequest>* stream) {
  MultiGetRequest request;
  while (stream->Read(&request)) {
    auto embedding = store_->get(request.key());
    std::unique_ptr<Embedding> proto_embedding(new Embedding());
    *proto_embedding->mutable_values() = {embedding.begin(), embedding.end()};
    MultiGetResponse resp;
    resp.set_allocated_embedding(proto_embedding.release());
    stream->Write(resp);
  }
  
  return Status::OK;
}

grpc::Status EmbeddingStoreService::NearestNeighbor(
    ServerContext* context, const NearestNeighborRequest* request,
    NearestNeighborResponse* resp) {
    auto space = store_->create_space(request->space(), 3);
    auto ref_key = request->key();
    auto ref_vec = space->get(ref_key);
    auto nearest = space->get_ann_index()->approx_nearest(ref_vec, request->num() + 1);
    if(!remove_uniq_value(nearest, request->key())) {
        nearest.pop_back();
    }
    *resp->mutable_keys() = {nearest.begin(), nearest.end()};
  return Status::OK;
}
}
}

using featureform::embedding::EmbeddingStore;
using featureform::embedding::EmbeddingStoreService;

void RunServer() {
  std::string server_address("0.0.0.0:50051");
  auto store = EmbeddingStore::load_or_create("embedding_store.dat");
  auto service = EmbeddingStoreService(std::move(store));

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
