# This Source Code Form is subject to the terms of the Mozilla Public
# License, v.2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@com_github_bazelbuild_buildtools//buildifier:def.bzl", "buildifier")
load("@com_github_bazelbuild_buildtools//buildifier:def.bzl", "buildifier_test")

buildifier(
    name = "buildifier",
)

buildifier_test(
    name = "buildifier_test",
    srcs = [":all_source_files"],
)

filegroup(
    name = "all_source_files",
    srcs = glob([
        "WORKSPACE",
        "BUILD",
        "*.md",
        "*.py",
        "*.sh",
        "pyproject.toml",
        "embddingstore:all_source_files",
        "thirdparty:all_source_files",
        "test:all_source_files",
        "client:all_source_files",
    ]),
    visibility = ["//visibility:public"],
)
