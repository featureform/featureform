load("@rules_foreign_cc//tools/build_defs:make.bzl", "make")

make(
    name = "rocksdb",
    lib_source = ":all",
    linkopts = [
        "-ldl",
        "-lz",
        "-lbz2"
    ],
    make_commands = ["make -j8 static_lib && mkdir -p $INSTALLDIR/lib && cp librocksdb.a $INSTALLDIR/lib && cp -r include/rocksdb $INSTALLDIR/rocksdb/"],
    out_include_dir = "rocksdb",
    static_libraries = ["librocksdb.a"],
    visibility = ["//visibility:public"],
)

filegroup(
    name = "all",
    srcs = glob(["**"]),
)
