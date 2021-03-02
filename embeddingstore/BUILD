# This Source Code Form is subject to the terms of the Mozilla Public
# License, v.2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@rules_proto_grpc//cpp:defs.bzl", "cpp_grpc_library")

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
        "@com_github_google_glog//:glog",
    ],
)

proto_library(
    name = "embedding_store_proto",
    srcs = [
        "embedding_store.proto",
    ],
    visibility = [
        "//visibility:public",
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
        ":controller",
        ":file_handler",
        ":embedding_store_grpc",
        "@com_github_google_glog//:glog",
    ],
)

cc_library(
    name = "controller",
    srcs =
        [
            "controller.cc",
        ],
    hdrs =
        [
            "controller.h",
        ],
    visibility = [
        "//visibility:public",
    ],
    deps = [
        ":embedding_store",
        "@rocksdb",
        "@com_github_google_glog//:glog",
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
        ":embedding_store_grpc",
        "@hnswlib",
        "@com_github_google_glog//:glog",
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
        "@rocksdb",
        "@com_github_google_glog//:glog",
    ],
)

cpp_grpc_library(
    name = "embedding_store_grpc",
    deps = [":embedding_store_proto"],
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
        ":index",
        ":storage",
        "@rocksdb",
        "@com_github_google_glog//:glog",
    ],
)

cc_library(
    name = "file_handler",
    srcs =
        [
            "file_handler.cc",
        ],
    hdrs =
        [
            "file_handler.h",
        ],
    visibility = [
        "//visibility:public",
    ],
    deps = [
    ],
)

