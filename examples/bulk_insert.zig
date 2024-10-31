const std = @import("std");
const ClickHouseClient = @import("../src/main.zig").ClickHouseClient;
const ClickHouseConfig = @import("../src/main.zig").ClickHouseConfig;
const BulkInsert = @import("../src/bulk_insert.zig").BulkInsert;
const compression = @import("../src/compression.zig");

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
        \\CREATE TABLE IF NOT EXISTS test_bulk (
        \\    id UInt32,
        \\    name String,
        \\    value Float64
        \\) ENGINE = MergeTree()
        \\ORDER BY id
    );

    // Initialize bulk insert
    const columns = [_]BulkInsert.ColumnDef{
        .{ .name = "id", .type_str = "UInt32" },
        .{ .name = "name", .type_str = "String" },
        .{ .name = "value", .type_str = "Float64" },
    };

    var bulk = try BulkInsert.init(allocator, "test_bulk", &columns, 1000);
    defer bulk.deinit();

    // Enable LZ4 compression
    bulk.setCompression(.LZ4);

    // Insert sample data
    var i: u32 = 0;
    while (i < 10000) : (i += 1) {
        const values = [_]BulkInsert.Value{
            .{ .UInt32 = i },
            .{ .String = "test" },
            .{ .Float64 = @intToFloat(f64, i) * 1.5 },
        };

        if (try bulk.addRow(&values)) {
            try bulk.flush();
        }
    }

    // Flush any remaining rows
    try bulk.flush();

    std.debug.print("Bulk insert completed successfully\n", .{});
}