# This Source Code Form is subject to the terms of the Mozilla Public
# License, v.2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@python_test_deps//:requirements.bzl", "requirement")

py_test(
    name = "offlinehub_test",
    srcs = ["offlinehub_test.py"],
    main = "offlinehub_test.py",
    deps = [
        requirement("pytest"),
        "//sdk/python:offlinehub",
    ],
)
