/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#include "server.h"

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include <algorithm>
#include <iostream>
#include <memory>
#include <string>

using ::featureform::embedding::proto::CreateSpaceRequest;
using ::featureform::embedding::proto::CreateSpaceResponse;
using ::featureform::embedding::proto::Embedding;
using ::featureform::embedding::proto::FreezeSpaceRequest;
using ::featureform::embedding::proto::FreezeSpaceResponse;
using ::featureform::embedding::proto::GetRequest;
using ::featureform::embedding::proto::GetResponse;
using ::featureform::embedding::proto::MultiGetRequest;
using ::featureform::embedding::proto::MultiGetResponse;
using ::featureform::embedding::proto::MultiSetRequest;
using ::featureform::embedding::proto::MultiSetResponse;
using ::featureform::embedding::proto::NearestNeighborRequest;
using ::featureform::embedding::proto::NearestNeighborResponse;
using ::featureform::embedding::proto::SetRequest;
using ::featureform::embedding::proto::SetResponse;
using ::grpc::Server;
using ::grpc::ServerBuilder;
using ::grpc::ServerContext;
using ::grpc::ServerReader;
using ::grpc::ServerReaderWriter;
using ::grpc::Status;
using ::grpc::StatusCode;

namespace featureform {

namespace embedding {

constexpr auto DEFAULT_VERSION = "initial";

std::vector<float> copy_embedding_to_vector(const Embedding& embedding) {
  auto vals = embedding.values();
  return std::vector<float>(vals.cbegin(), vals.cend());
}

template <typename T>
bool remove_uniq_value(std::vector<T>& vec, T val) {
  auto pos = std::find(vec.begin(), vec.end(), val);
  if (pos != vec.end()) {
    vec.erase(pos);
    return true;
  }
  return false;
}

grpc::Status EmbeddingHubService::CreateSpace(ServerContext* context,
                                              const CreateSpaceRequest* request,
                                              CreateSpaceResponse* resp) {
  std::unique_lock<std::mutex> lock(mtx_);
  auto space = store_->create_space(request->name());
  space->create_version(DEFAULT_VERSION, request->dims());
  return Status::OK;
}

grpc::Status EmbeddingHubService::FreezeSpace(ServerContext* context,
                                              const FreezeSpaceRequest* request,
                                              FreezeSpaceResponse* resp) {
  std::unique_lock<std::mutex> lock(mtx_);
  auto space_opt = store_->get_space(request->name());
  if (!space_opt) {
    return Status(StatusCode::NOT_FOUND, "Not found");
  }
  auto version_opt = GetVersion(request->name(), DEFAULT_VERSION);
  if (!version_opt.has_value()) {
    return Status(StatusCode::NOT_FOUND, "Not found");
  }
  version_opt.value()->make_immutable();
  return Status::OK;
}

grpc::Status EmbeddingHubService::Get(ServerContext* context,
                                      const GetRequest* request,
                                      GetResponse* resp) {
  std::unique_lock<std::mutex> lock(mtx_);
  auto version_opt = GetVersion(request->space(), DEFAULT_VERSION);
  if (!version_opt.has_value()) {
    return Status(StatusCode::NOT_FOUND, "Not found");
  }
  auto embedding = version_opt.value()->get(request->key());
  std::unique_ptr<Embedding> proto_embedding(new Embedding());
  *proto_embedding->mutable_values() = {embedding.begin(), embedding.end()};
  resp->set_allocated_embedding(proto_embedding.release());
  return Status::OK;
}

grpc::Status EmbeddingHubService::Set(ServerContext* context,
                                      const SetRequest* request,
                                      SetResponse* resp) {
  std::unique_lock<std::mutex> lock(mtx_);
  auto vec = copy_embedding_to_vector(request->embedding());
  auto version_opt = GetVersion(request->space(), DEFAULT_VERSION);
  if (!version_opt.has_value()) {
    return Status(StatusCode::NOT_FOUND, "Not found");
  }
  auto err = version_opt.value()->set(request->key(), vec);

  if (err != nullptr) {
    return Status(StatusCode::FAILED_PRECONDITION,
                  "Cannot write to immutable space");
  }
  return Status::OK;
}

grpc::Status EmbeddingHubService::MultiSet(
    ServerContext* context, ServerReader<MultiSetRequest>* reader,
    MultiSetResponse* resp) {
  std::unique_lock<std::mutex> lock(mtx_);
  MultiSetRequest request;
  while (reader->Read(&request)) {
    auto vec = copy_embedding_to_vector(request.embedding());
    auto version_opt = GetVersion(request.space(), DEFAULT_VERSION);
    if (!version_opt.has_value()) {
      return Status(StatusCode::NOT_FOUND, "Not found");
    }
    auto err = (*version_opt)->set(request.key(), vec);
    if (err != nullptr) {
      return Status(StatusCode::FAILED_PRECONDITION,
                    "Cannot write to immutable space");
    }
  }
  return Status::OK;
}

grpc::Status EmbeddingHubService::MultiGet(
    ServerContext* context,
    ServerReaderWriter<MultiGetResponse, MultiGetRequest>* stream) {
  std::unique_lock<std::mutex> lock(mtx_);
  MultiGetRequest request;
  while (stream->Read(&request)) {
    auto version_opt = GetVersion(request.space(), DEFAULT_VERSION);
    if (!version_opt.has_value()) {
      return Status(StatusCode::NOT_FOUND, "Not found");
    }
    auto embedding = (*version_opt)->get(request.key());
    std::unique_ptr<Embedding> proto_embedding(new Embedding());
    *proto_embedding->mutable_values() = {embedding.begin(), embedding.end()};
    MultiGetResponse resp;
    resp.set_allocated_embedding(proto_embedding.release());
    stream->Write(resp);
  }

  return Status::OK;
}

grpc::Status EmbeddingHubService::NearestNeighbor(
    ServerContext* context, const NearestNeighborRequest* request,
    NearestNeighborResponse* resp) {
  std::unique_lock<std::mutex> lock(mtx_);
  auto version_opt = GetVersion(request->space(), DEFAULT_VERSION);
  if (!version_opt.has_value()) {
    return Status(StatusCode::NOT_FOUND, "Not found");
  }
  auto version = *version_opt;
  auto ref_key = request->key();
  auto ref_vec = version->get(ref_key);
  auto nearest =
      version->get_ann_index()->approx_nearest(ref_vec, request->num() + 1);
  if (!remove_uniq_value(nearest, request->key())) {
    nearest.pop_back();
  }
  *resp->mutable_keys() = {nearest.begin(), nearest.end()};
  return Status::OK;
}

std::optional<std::shared_ptr<Version>> EmbeddingHubService::GetVersion(
    const std::string& space_name, const std::string& version_name) {
  auto space_opt = store_->get_space(space_name);
  if (!space_opt.has_value()) {
    return std::nullopt;
  }
  return (*space_opt)->get_version(version_name);
}
}  // namespace embedding
}  // namespace featureform

using featureform::embedding::EmbeddingHub;
using featureform::embedding::EmbeddingHubService;

void RunServer() {
  std::string server_address("0.0.0.0:50051");
  auto store = EmbeddingHub::load_or_create("embedding_store.dat");
  auto service = EmbeddingHubService(std::move(store));

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
