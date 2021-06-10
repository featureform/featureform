# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

# This file is used to setup external dependencies.

# http_archive and new_git_repository are used to pull external source
# directories.
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "new_git_repository")

# gtest is a testing framework for C++.
http_archive(
    name = "gtest",
    sha256 = "9dc9157a9a1551ec7a7e43daea9a694a0bb5fb8bec81235d8a1e6ef64c716dcb",
    strip_prefix = "googletest-release-1.10.0",
    url = "https://github.com/google/googletest/archive/release-1.10.0.tar.gz",
)

http_archive(
    name = "rules_python",
    sha256 = "b6d46438523a3ec0f3cead544190ee13223a52f6a6765a29eae7b7cc24cc83a0",
    url = "https://github.com/bazelbuild/rules_python/releases/download/0.1.0/rules_python-0.1.0.tar.gz",
)

git_repository(
    name = "rules_proto",
    commit = "cfdc2fa31879c0aebe31ce7702b1a9c8a4be02d2",
    remote = "https://github.com/bazelbuild/rules_proto.git",
)

load("@rules_proto//proto:repositories.bzl", "rules_proto_dependencies", "rules_proto_toolchains")

rules_proto_dependencies()

rules_proto_toolchains()

http_archive(
    name = "com_github_grpc_grpc",
    strip_prefix = "grpc-1.38.0",
    urls = [
        "https://github.com/grpc/grpc/archive/v1.38.0.tar.gz",
    ],
)

load("@com_github_grpc_grpc//bazel:grpc_deps.bzl", "grpc_deps")

grpc_deps()

load("@com_github_grpc_grpc//bazel:grpc_extra_deps.bzl", "grpc_extra_deps")

grpc_extra_deps()

load("@com_github_grpc_grpc//bazel:grpc_extra_deps.bzl", "grpc_extra_deps")

grpc_extra_deps()

load("@com_github_grpc_grpc//bazel:grpc_python_deps.bzl", "grpc_python_deps")

grpc_python_deps()

# Add rules to compile external C++ projects that use Make or CMake.
http_archive(
    name = "rules_foreign_cc",
    sha256 = "d54742ffbdc6924f222d2179f0e10e911c5c659c4ae74158e9fe827aad862ac6",
    strip_prefix = "rules_foreign_cc-0.2.0",
    url = "https://github.com/bazelbuild/rules_foreign_cc/archive/0.2.0.tar.gz",
)

load("@rules_foreign_cc//foreign_cc:repositories.bzl", "rules_foreign_cc_dependencies")

rules_foreign_cc_dependencies()

# Using git_remote instead of recommended http_archive on purpose.
# At time of commit, hnswlib's newest release is 0.4.0 which is missing
# certain critical commits such as:
# https://github.com/nmslib/hnswlib/commit/40f31dad9508dac53f3201eb17fe5dcc72eb1d43
#
# Without these commits, the project fails to compile.
new_git_repository(
    name = "hnswlib",
    build_file = "@//thirdparty/hnswlib:BUILD.bzl",
    commit = "21b54fe9544cfbb757b2ea8f3def5542ba2435c7",
    remote = "https://github.com/nmslib/hnswlib.git",
)

http_archive(
    name = "expected",
    build_file = "@//thirdparty/expected:BUILD.bzl",
    sha256 = "8f5124085a124113e75e3890b4e923e3a4de5b26a973b891b3deb40e19c03cee",
    strip_prefix = "expected-1.0.0",
    url = "https://github.com/TartanLlama/expected/archive/v1.0.0.tar.gz",
)

http_archive(
    name = "rocksdb",
    build_file = "@//thirdparty/rocksdb:BUILD.bzl",
    sha256 = "c6502c7aae641b7e20fafa6c2b92273d935d2b7b2707135ebd9a67b092169dca",
    strip_prefix = "rocksdb-6.20.3",
    url = "https://github.com/facebook/rocksdb/archive/v6.20.3.tar.gz",
)

# buildifier is written in Go and hence needs rules_go to be built.
# See https://github.com/bazelbuild/rules_go for the up to date setup instructions.
http_archive(
    name = "io_bazel_rules_go",
    sha256 = "ac03931e56c3b229c145f1a8b2a2ad3e8d8f1af57e43ef28a26123362a1e3c7e",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_go/releases/download/v0.24.4/rules_go-v0.24.4.tar.gz",
        "https://github.com/bazelbuild/rules_go/releases/download/v0.24.4/rules_go-v0.24.4.tar.gz",
    ],
)

load("@io_bazel_rules_go//go:deps.bzl", "go_register_toolchains", "go_rules_dependencies")

go_rules_dependencies()

go_register_toolchains()

http_archive(
    name = "com_github_bazelbuild_buildtools",
    sha256 = "c28eef4d30ba1a195c6837acf6c75a4034981f5b4002dda3c5aa6e48ce023cf1",
    strip_prefix = "buildtools-4.0.1",
    url = "https://github.com/bazelbuild/buildtools/archive/4.0.1.tar.gz",
)

load("@rules_python//python:pip.bzl", "pip_install")

pip_install(
    name = "integration_py_deps",
    python_interpreter = "python3",
    requirements = "//test:requirements.txt",
)
