const std = @import("std");
const ClickHouseClient = @import("main.zig").ClickHouseClient;
const ClickHouseConfig = @import("main.zig").ClickHouseConfig;

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
    try client.query("SELECT 1");

    std.debug.print("Query executed successfully\n", .{});
}