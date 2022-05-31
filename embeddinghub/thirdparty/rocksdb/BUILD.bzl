load("@rules_foreign_cc//foreign_cc:make.bzl", "make")

make(
    name = "rocksdb",
    lib_source = ":all",
    linkopts = [
        "-ldl",
        "-lz",
        "-lzstd",
        "-lbz2",
        "-llz4",
        "-lsnappy",
    ],
    tool_prefix = "PORTABLE=1",
    targets = ["install-static"],
    args = ["-j8"],
    postfix_script = "cp -L -R include $$INSTALLDIR$$/",
    out_include_dir = "include",
    out_static_libs = ["librocksdb.a"],
    visibility = ["//visibility:public"],
)

filegroup(
    name = "all",
    srcs = glob(["**"]),
)
