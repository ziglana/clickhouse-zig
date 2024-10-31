const std = @import("std");
const AsyncClient = @import("../src/async_client.zig").AsyncClient;
const ClickHouseConfig = @import("../src/main.zig").ClickHouseConfig;
const AsyncQuery = @import("../src/async_query.zig").AsyncQuery;

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

    var client = AsyncClient.init(allocator, config);
    defer client.deinit();

    try client.connect();

    // Create async query
    var query = AsyncQuery.init(
        allocator,
        &client,
        "SELECT number FROM system.numbers LIMIT 10000000"
    );
    defer query.deinit();

    // Execute query with callback
    try query.executeWithCallback(queryCallback);

    std.debug.print("Async query started\n", .{});
}

fn queryCallback(query: *AsyncQuery, result: ?*results.QueryResult, err: ?*error.Error) void {
    if (err) |e| {
        std.debug.print("Query error: {}\n", .{e});
        return;
    }

    if (result) |r| {
        std.debug.print("Query completed with {} rows\n", .{r.rows});
    }
}