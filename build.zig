const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Library
    const lib = b.addStaticLibrary(.{
        .name = "clickhouse-zig",
        .root_source_file = .{ .path = "src/main.zig" },
        .target = target,
        .optimize = optimize,
    });

    // Add LZ4 and ZSTD as dependencies
    const lz4_lib = b.addStaticLibrary(.{
        .name = "lz4",
        .target = target,
        .optimize = optimize,
    });
    lz4_lib.addCSourceFile("deps/lz4/lib/lz4.c", &[_][]const u8{"-std=c99"});
    lz4_lib.addIncludePath("deps/lz4/lib");
    lib.linkLibrary(lz4_lib);

    const zstd_lib = b.addStaticLibrary(.{
        .name = "zstd",
        .target = target,
        .optimize = optimize,
    });
    zstd_lib.addCSourceFiles(&.{
        "deps/zstd/lib/common/entropy_common.c",
        "deps/zstd/lib/common/error_private.c",
        "deps/zstd/lib/common/fse_decompress.c",
        "deps/zstd/lib/common/pool.c",
        "deps/zstd/lib/common/threading.c",
        "deps/zstd/lib/common/xxhash.c",
        "deps/zstd/lib/common/zstd_common.c",
        "deps/zstd/lib/compress/fse_compress.c",
        "deps/zstd/lib/compress/hist.c",
        "deps/zstd/lib/compress/huf_compress.c",
        "deps/zstd/lib/compress/zstd_compress.c",
        "deps/zstd/lib/compress/zstd_double_fast.c",
        "deps/zstd/lib/compress/zstd_fast.c",
        "deps/zstd/lib/compress/zstd_lazy.c",
        "deps/zstd/lib/compress/zstd_ldm.c",
        "deps/zstd/lib/compress/zstd_opt.c",
        "deps/zstd/lib/decompress/huf_decompress.c",
        "deps/zstd/lib/decompress/zstd_decompress.c",
    }, &[_][]const u8{"-std=c99"});
    zstd_lib.addIncludePath("deps/zstd/lib");
    lib.linkLibrary(zstd_lib);

    b.installArtifact(lib);

    // Tests
    const main_tests = b.addTest(.{
        .root_source_file = .{ .path = "src/main.zig" },
        .target = target,
        .optimize = optimize,
    });
    main_tests.linkLibrary(lz4_lib);
    main_tests.linkLibrary(zstd_lib);

    const run_main_tests = b.addRunArtifact(main_tests);

    const test_step = b.step("test", "Run library tests");
    test_step.dependOn(&run_main_tests.step);

    // Examples
    const examples = .{
        "basic_connection",
        "show_databases",
        "database_size",
        "show_connections",
    };

    inline for (examples) |example_name| {
        const example = b.addExecutable(.{
            .name = example_name,
            .root_source_file = .{ .path = "examples/" ++ example_name ++ ".zig" },
            .target = target,
            .optimize = optimize,
        });
        example.addModule("clickhouse", lib.getModule());
        example.linkLibrary(lz4_lib);
        example.linkLibrary(zstd_lib);
        
        const run_cmd = b.addRunArtifact(example);
        const run_step = b.step("run-" ++ example_name, "Run the " ++ example_name ++ " example");
        run_step.dependOn(&run_cmd.step);
        
        b.installArtifact(example);
    }
}