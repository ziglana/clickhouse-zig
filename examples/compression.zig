const std = @import("std");
const ClickHouseClient = @import("../src/main.zig").ClickHouseClient;
const ClickHouseConfig = @import("../src/main.zig").ClickHouseConfig;
const compression = @import("../src/compression.zig");
const BulkInsert = @import("../src/bulk_insert.zig").BulkInsert;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const config = ClickHouseConfig{
        .host = "localhost",
        .port = 9000,
        .username = "default",
        .password = "",
        .database = "default",
    };

    var client = ClickHouseClient.init(allocator, config);
    defer client.deinit();

    try client.connect();

    // Create test table
    try client.query(
        \\CREATE TABLE IF NOT EXISTS test_compression (
        \\    id UInt32,
        \\    data String,
        \\    numbers Array(Float64)
        \\) ENGINE = MergeTree()
        \\ORDER BY id
    );

    // Test different compression methods
    const compression_methods = [_]compression.CompressionMethod{
        .None,
        .LZ4,
        .ZSTD,
    };

    // Generate sample data
    var large_string = std.ArrayList(u8).init(allocator);
    defer large_string.deinit();
    var i: usize = 0;
    while (i < 10000) : (i += 1) {
        try large_string.writer().print("This is test data line {}\n", .{i});
    }

    var numbers = std.ArrayList(f64).init(allocator);
    defer numbers.deinit();
    i = 0;
    while (i < 1000) : (i += 1) {
        try numbers.append(@intToFloat(f64, i) * 1.5);
    }

    // Test each compression method
    for (compression_methods) |method| {
        // Initialize bulk insert with compression
        const columns = [_]BulkInsert.ColumnDef{
            .{ .name = "id", .type_str = "UInt32" },
            .{ .name = "data", .type_str = "String" },
            .{ .name = "numbers", .type_str = "Array(Float64)" },
        };

        var bulk = try BulkInsert.init(allocator, "test_compression", &columns, 1000);
        defer bulk.deinit();

        // Set compression method
        bulk.setCompression(method);

        // Insert data
        const values = [_]BulkInsert.Value{
            .{ .UInt32 = 1 },
            .{ .String = large_string.items },
            .{ .Array = numbers.items },
        };

        const start = std.time.milliTimestamp();
        
        if (try bulk.addRow(&values)) {
            try bulk.flush();
        }

        const end = std.time.milliTimestamp();

        // Compress the data separately to measure compression ratio
        const compressed = try compression.CompressedData.compress(
            allocator,
            large_string.items,
            method,
        );
        defer compressed.deinit(allocator);

        const ratio = @intToFloat(f64, large_string.items.len) / 
                     @intToFloat(f64, compressed.compressed_size);

        std.debug.print("\nCompression Method: {s}\n", .{@tagName(method)});
        std.debug.print("Time: {}ms\n", .{end - start});
        std.debug.print("Original Size: {} bytes\n", .{large_string.items.len});
        std.debug.print("Compressed Size: {} bytes\n", .{compressed.compressed_size});
        std.debug.print("Compression Ratio: {d:.2}x\n", .{ratio});
    }

    std.debug.print("\nCompression example completed successfully\n", .{});
}