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
    make_commands = ["make -j8 static_lib && mv librocksdb.a $INSTALLDIR/lib && cp -L -r include/rocksdb $INSTALLDIR/rocksdb/"],
    out_include_dir = "rocksdb",
    static_libraries = ["librocksdb.a"],
    visibility = ["//visibility:public"],
)

filegroup(
    name = "all",
    srcs = glob(["**"]),
)
