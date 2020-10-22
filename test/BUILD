# This Source Code Form is subject to the terms of the Mozilla Public
# License, v.2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@integration_py_deps//:requirements.bzl", "requirement")

py_test(
    name = "integration",
    srcs = ["integration.py"],
    data = ["//embeddingstore:main"],
    main = "integration.py",
    deps = [
        requirement("pytest"),
        "//client:lib",
    ],
)
