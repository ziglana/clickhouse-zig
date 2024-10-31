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
    
    // Query to get database sizes in GB
    const size_query = 
        \\SELECT
        \\    database,
        \\    formatReadableSize(sum(bytes)) AS size,
        \\    round(sum(bytes) / pow(1024, 3), 2) AS size_gb
        \\FROM system.parts
        \\GROUP BY database
        \\ORDER BY sum(bytes) DESC
    ;
    
    try client.query(size_query);

    std.debug.print("Query executed: Database sizes retrieved\n", .{});
}