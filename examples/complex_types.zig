const std = @import("std");
const ClickHouseClient = @import("../src/main.zig").ClickHouseClient;
const ClickHouseConfig = @import("../src/main.zig").ClickHouseConfig;
const complex_types = @import("../src/complex_types.zig");
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

    // Create table with complex types
    try client.query(
        \\CREATE TABLE IF NOT EXISTS test_complex (
        \\    id UInt32,
        \\    tags Array(String),
        \\    properties Map(String, String),
        \\    nested Nested(
        \\        key String,
        \\        value Float64
        \\    ),
        \\    category LowCardinality(String),
        \\    location Point,
        \\    area Polygon,
        \\    metadata JSON
        \\) ENGINE = MergeTree()
        \\ORDER BY id
    );

    // Prepare complex data types
    var tags = [_][]const u8{ "tag1", "tag2", "tag3" };
    
    var properties = std.StringHashMap([]const u8).init(allocator);
    defer properties.deinit();
    try properties.put("color", "red");
    try properties.put("size", "large");

    var nested = complex_types.Nested.init(
        allocator,
        "metrics",
        &[_]types.TypeInfo{
            .{ .base_type = .String },
            .{ .base_type = .Float64 },
        },
    );
    defer nested.deinit();

    var location = complex_types.Point{
        .x = 10.5,
        .y = 20.7,
    };

    var area = try complex_types.Polygon.init(
        allocator,
        &[_]complex_types.Ring{
            try complex_types.Ring.init(allocator, &[_]complex_types.Point{
                .{ .x = 0, .y = 0 },
                .{ .x = 10, .y = 0 },
                .{ .x = 10, .y = 10 },
                .{ .x = 0, .y = 10 },
                .{ .x = 0, .y = 0 },
            }),
        },
    );
    defer area.deinit();

    var metadata = try complex_types.JSON.init(
        allocator,
        \\{
        \\    "version": 1,
        \\    "description": "Test object",
        \\    "enabled": true
        \\}
    );
    defer metadata.deinit();

    // Insert data using bulk insert
    const columns = [_]BulkInsert.ColumnDef{
        .{ .name = "id", .type_str = "UInt32" },
        .{ .name = "tags", .type_str = "Array(String)" },
        .{ .name = "properties", .type_str = "Map(String, String)" },
        .{ .name = "nested", .type_str = "Nested(key String, value Float64)" },
        .{ .name = "category", .type_str = "LowCardinality(String)" },
        .{ .name = "location", .type_str = "Point" },
        .{ .name = "area", .type_str = "Polygon" },
        .{ .name = "metadata", .type_str = "JSON" },
    };

    var bulk = try BulkInsert.init(allocator, "test_complex", &columns, 1000);
    defer bulk.deinit();

    // Insert sample row
    const values = [_]BulkInsert.Value{
        .{ .UInt32 = 1 },
        .{ .Array = &tags },
        .{ .Map = properties },
        .{ .Nested = nested },
        .{ .String = "category_a" },
        .{ .Point = location },
        .{ .Polygon = area },
        .{ .JSON = metadata },
    };

    if (try bulk.addRow(&values)) {
        try bulk.flush();
    }

    std.debug.print("Complex types example completed successfully\n", .{});
}