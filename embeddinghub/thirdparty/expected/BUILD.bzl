# This Source Code Form is subject to the terms of the Mozilla Public
# License, v.2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

cc_library(
    name = "expected",
    hdrs = ["tl/expected.h"],
    visibility = ["//visibility:public"],
)

genrule(
    name = "header_mv",
    srcs = ["include/tl/expected.hpp"],
    outs = ["tl/expected.h"],
    cmd = "mkdir tl && mv $(location include/tl/expected.hpp) $(location tl/expected.h)",
)
