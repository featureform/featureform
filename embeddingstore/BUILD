# This Source Code Form is subject to the terms of the Mozilla Public
# License, v.2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@rules_proto//proto:defs.bzl", "proto_library")
load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_proto_library")
load("@com_github_grpc_grpc//bazel:cc_grpc_library.bzl", "cc_grpc_library")

cc_binary(
    name = "main",
    srcs =
        [
            "main.cc",
        ],
    visibility = [
        "//visibility:public",
    ],
    deps = [
        ":server",
    ],
)

cc_library(
    name = "server",
    srcs =
        [
            "server.cc",
        ],
    hdrs =
        [
            "server.h",
        ],
    visibility = [
        "//visibility:public",
    ],
    deps = [
        ":embedding_store",
        ":embedding_store_grpc",
        "@com_github_grpc_grpc//:grpc++",
        "@com_github_grpc_grpc//:grpc++_reflection",
    ],
)

cc_library(
    name = "index",
    srcs =
        [
            "index.cc",
        ],
    hdrs =
        [
            "index.h",
        ],
    visibility = [
        "//visibility:public",
    ],
    deps = [
        "@hnswlib",
    ],
)

cc_library(
    name = "storage",
    srcs =
        [
            "storage.cc",
        ],
    hdrs =
        [
            "storage.h",
        ],
    visibility = [
        "//visibility:public",
    ],
    deps = [
        ":embedding_store_grpc",
        "@com_github_grpc_grpc//:grpc++",
        "@rocksdb",
    ],
)

proto_library(
    name = "embedding_store_proto",
    srcs = ["embedding_store.proto"],
    visibility = [
        "//visibility:public",
    ],
)

cc_proto_library(
    name = "embedding_store_cc_proto",
    deps = [":embedding_store_proto"],
)

cc_grpc_library(
    name = "embedding_store_grpc",
    srcs = [":embedding_store_proto"],
    grpc_only = True,
    deps = [":embedding_store_cc_proto"],
)

proto_library(
    name = "embedding_store_meta_proto",
    srcs = ["embedding_store_meta.proto"],
    visibility = [
        "//visibility:public",
    ],
)

cc_proto_library(
    name = "embedding_store_meta_cc_proto",
    deps = [":embedding_store_meta_proto"],
)

cc_library(
    name = "space",
    srcs =
        [
            "space.cc",
        ],
    hdrs =
        [
            "space.h",
        ],
    visibility = [
        "//visibility:public",
    ],
    deps = [
        ":index",
        ":storage",
        "@rocksdb",
    ],
)

cc_library(
    name = "embedding_store",
    srcs =
        [
            "embedding_store.cc",
        ],
    hdrs =
        [
            "embedding_store.h",
        ],
    visibility = [
        "//visibility:public",
    ],
    deps = [
        ":embedding_store_meta_cc_proto",
        ":space",
    ],
)

cc_library(
    name = "metadata",
    srcs =
        [
            "metadata.cc",
        ],
    hdrs =
        [
            "metadata.h",
        ],
    visibility = [
        "//visibility:public",
    ],
    deps = [
        ":embedding_store_meta_cc_proto",
        ":error",
        "@expected",
        "@rocksdb",
    ],
)

cc_library(
    name = "error",
    hdrs =
        [
            "error.h",
        ],
    visibility = [
        "//visibility:public",
    ],
    deps = [
        "@rocksdb",
    ],
)

filegroup(
    name = "all_source_files",
    srcs = glob([
        "BUILD",
        "*.cc",
        "*.h",
        "*.proto",
        "test:all_source_files",
    ]),
    visibility = ["//visibility:public"],
)
