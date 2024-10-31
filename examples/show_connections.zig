const std = @import("std");
const ClickHouseClient = @import("../src/main.zig").ClickHouseClient;
const ClickHouseConfig = @import("../src/main.zig").ClickHouseConfig;

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
    
    // Query to show current connections
    const connections_query = 
        \\SELECT
        \\    user,
        \\    address,
        \\    client_name,
        \\    client_version,
        \\    query_duration_ms,
        \\    query
        \\FROM system.processes
        \\ORDER BY query_duration_ms DESC
    ;
    
    try client.query(connections_query);

    std.debug.print("Query executed: Current connections retrieved\n", .{});
}