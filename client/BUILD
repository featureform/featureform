# This Source Code Form is subject to the terms of the Mozilla Public
# License, v.2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@rules_proto_grpc//python:defs.bzl", "python_grpc_library")

py_binary(
    name = "client",
    srcs = ["client.py"],
    main = "client.py",
    deps = [":embedding_store_grpc"],
)

python_grpc_library(
    name = "embedding_store_grpc",
    deps = ["//embeddingstore:embedding_store_proto"],
)
