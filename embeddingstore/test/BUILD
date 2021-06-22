# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

cc_test(
    name = "embedding_store_test",
    srcs = [
        "embedding_store_test.cc",
        "main_test.cc",
    ],
    deps = [
        "//embeddingstore:embedding_store",
        "@gtest",
    ],
)

cc_test(
    name = "metadata_test",
    srcs = [
        "main_test.cc",
        "metadata_test.cc",
    ],
    deps = [
        "//embeddingstore:metadata",
        "@gtest",
    ],
)

cc_test(
    name = "space_test",
    srcs = [
        "main_test.cc",
        "space_test.cc",
    ],
    deps = [
        "//embeddingstore:space",
        "@gtest",
    ],
)

cc_test(
    name = "index_test",
    srcs = [
        "index_test.cc",
        "main_test.cc",
    ],
    deps = [
        "//embeddingstore:index",
        "@gtest",
    ],
)

cc_test(
    name = "storage_test",
    srcs = [
        "main_test.cc",
        "storage_test.cc",
    ],
    deps = [
        "//embeddingstore:storage",
        "@gtest",
    ],
)

filegroup(
    name = "all_source_files",
    srcs = glob([
        "BUILD",
        "*.cc",
        "*.h",
        "*.proto",
    ]),
    visibility = ["//visibility:public"],
)
