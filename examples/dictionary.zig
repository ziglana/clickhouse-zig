const std = @import("std");
const ClickHouseClient = @import("../src/main.zig").ClickHouseClient;
const ClickHouseConfig = @import("../src/main.zig").ClickHouseConfig;
const Dictionary = @import("../src/dictionary.zig").Dictionary;

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

    // Create source table for dictionary
    try client.query(
        \\CREATE TABLE IF NOT EXISTS user_data (
        \\    user_id UInt32,
        \\    name String,
        \\    country_code String,
        \\    created_at DateTime
        \\) ENGINE = MergeTree()
        \\ORDER BY user_id
    );

    // Define dictionary structure
    const attributes = [_]Dictionary.DictionaryAttribute{
        .{ .name = "name", .type = "String" },
        .{ .name = "country_code", .type = "String" },
        .{ .name = "created_at", .type = "DateTime" },
    };

    // Create dictionary
    var dict = try Dictionary.init(
        allocator,
        "user_dictionary",
        .{ .ClickHouse = .{
            .db = "default",
            .table = "user_data",
        }},
        .ComplexKeyHashTable,
        .{ .TTL = 3600 }, // 1 hour
        &attributes
    );
    defer dict.deinit();

    // Create the dictionary
    try dict.create(&client);

    std.debug.print("Dictionary created successfully\n", .{});
}