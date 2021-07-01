# This Source Code Form is subject to the terms of the Mozilla Public
# License, v.2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@com_github_grpc_grpc//bazel:python_rules.bzl", "py_grpc_library")
load("@com_github_grpc_grpc//bazel:python_rules.bzl", "py_proto_library")

py_binary(
    name = "repl",
    srcs = [
        "repl.py",
    ],
    main = "repl.py",
    deps = [
        ":client",
        "//client:embedding_store_grpc",
    ],
)

py_library(
    name = "client",
    srcs = ["embeddinghub.py"],
    visibility = [
        "//visibility:public",
    ],
    deps = ["//client:embedding_store_grpc"],
)

py_grpc_library(
    name = "embedding_store_grpc",
    srcs = ["//embeddingstore:embedding_store_proto"],
    visibility = [
        "//visibility:public",
    ],
    deps = [":embedding_store_py_proto"],
)

py_proto_library(
    name = "embedding_store_py_proto",
    visibility = [
        "//visibility:public",
    ],
    deps = ["//embeddingstore:embedding_store_proto"],
)

filegroup(
    name = "all_source_files",
    srcs = glob([
        "BUILD",
        "*.py",
    ]),
    visibility = ["//visibility:public"],
)
