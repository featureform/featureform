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

http_archive(
    name = "rules_proto_grpc",
    sha256 = "d771584bbff98698e7cb3cb31c132ee206a972569f4dc8b65acbdd934d156b33",
    strip_prefix = "rules_proto_grpc-2.0.0",
    urls = ["https://github.com/rules-proto-grpc/rules_proto_grpc/archive/2.0.0.tar.gz"],
)

http_archive(
    name = "com_github_grpc_grpc",
    sha256 = "27dd2fc5c9809ddcde8eb6fa1fa278a3486566dfc28335fca13eb8df8bd3b958",
    strip_prefix = "grpc-1.35.0",
    urls = ["https://github.com/grpc/grpc/archive/v1.35.0.tar.gz"],
)

http_archive(
    name = "rules_proto",
    sha256 = "602e7161d9195e50246177e7c55b2f39950a9cf7366f74ed5f22fd45750cd208",
    strip_prefix = "rules_proto-97d8af4dc474595af3900dd85cb3a29ad28cc313",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_proto/archive/97d8af4dc474595af3900dd85cb3a29ad28cc313.tar.gz",
        "https://github.com/bazelbuild/rules_proto/archive/97d8af4dc474595af3900dd85cb3a29ad28cc313.tar.gz",
    ],
)

http_archive(
    name = "rules_proto_grpc",
    sha256 = "d771584bbff98698e7cb3cb31c132ee206a972569f4dc8b65acbdd934d156b33",
    strip_prefix = "rules_proto_grpc-2.0.0",
    urls = ["https://github.com/rules-proto-grpc/rules_proto_grpc/archive/2.0.0.tar.gz"],
)

load("@rules_proto_grpc//python:repositories.bzl", rules_proto_grpc_python_repos = "python_repos")

rules_proto_grpc_python_repos()

load("@rules_proto_grpc//:repositories.bzl", "rules_proto_grpc_repos", "rules_proto_grpc_toolchains")

rules_proto_grpc_toolchains()

rules_proto_grpc_repos()

load("@rules_proto//proto:repositories.bzl", "rules_proto_dependencies", "rules_proto_toolchains")

rules_proto_dependencies()

rules_proto_toolchains()

load("@rules_proto_grpc//cpp:repositories.bzl", rules_proto_grpc_cpp_repos = "cpp_repos")

rules_proto_grpc_cpp_repos()

load("@com_github_grpc_grpc//bazel:grpc_deps.bzl", "grpc_deps")

grpc_deps()

# Add rules to compile external C++ projects that use Make or CMake.
git_repository(
    name = "rules_foreign_cc",
    commit = "ed95b95affecaa3ea3bf7bab3e0ab6aa847dfb06",
    remote = "https://github.com/bazelbuild/rules_foreign_cc.git",
)

load("@rules_foreign_cc//:workspace_definitions.bzl", "rules_foreign_cc_dependencies")

rules_foreign_cc_dependencies()

# Using git_remote instead of recommended http_archive on purpose.
# At time of commit, hnswlib's newest release is 0.4.0 which is missing
# certain critical commits such as:
# https://github.com/nmslib/hnswlib/commit/40f31dad9508dac53f3201eb17fe5dcc72eb1d43
#
# Without these commits, the project fails to compile.
new_git_repository(
    name = "hnswlib",
    build_file = "@//thirdparty/hnswlib:BUILD",
    commit = "21b54fe9544cfbb757b2ea8f3def5542ba2435c7",
    remote = "https://github.com/nmslib/hnswlib.git",
)

http_archive(
    name = "rocksdb",
    build_file = "@//thirdparty/rocksdb:BUILD",
    sha256 = "fee38528108b6b49f813b9f055584b123ff0debbc3f39584d4a663c46075a6e9",
    strip_prefix = "rocksdb-6.12.7",
    url = "https://github.com/facebook/rocksdb/archive/v6.12.7.tar.gz",
)

http_archive(
    name = "com_github_gflags_gflags",
    sha256 = "34af2f15cf7367513b352bdcd2493ab14ce43692d2dcd9dfc499492966c64dcf",
    strip_prefix = "gflags-2.2.2",
    urls = ["https://github.com/gflags/gflags/archive/v2.2.2.tar.gz"],
)

http_archive(
    name = "com_github_google_glog",
    sha256 = "62efeb57ff70db9ea2129a16d0f908941e355d09d6d83c9f7b18557c0a7ab59e",
    strip_prefix = "glog-d516278b1cd33cd148e8989aec488b6049a4ca0b",
    urls = ["https://github.com/google/glog/archive/d516278b1cd33cd148e8989aec488b6049a4ca0b.zip"],
)

http_archive(
    name = "zipper",
    sha256 = "62efeb57ff70db9ea2129a16d0f908941e355d09d6d83c9f7b18557c0a7ab59e",
    strip_prefix = "glog-d516278b1cd33cd148e8989aec488b6049a4ca0b",
    urls = ["https://github.com/google/glog/archive/d516278b1cd33cd148e8989aec488b6049a4ca0b.zip"],
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
    sha256 = "1dbb1f39c17b1cbc011cc22394e6e88b0de13ad101eb40047c603297286c8398",
    strip_prefix = "buildtools-master",
    url = "https://github.com/bazelbuild/buildtools/archive/master.zip",
)

load("@rules_python//python:pip.bzl", "pip_install")

pip_install(
    name = "integration_py_deps",
    python_interpreter = "python3",
    requirements = "//test:requirements.txt",
)
