const std = @import("std");
const Pool = @import("../src/pool.zig").Pool;
const PoolConfig = @import("../src/pool.zig").PoolConfig;
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

    const pool_config = PoolConfig{
        .min_connections = 2,
        .max_connections = 5,
        .connection_timeout_ms = 5000,
    };

    var pool = try Pool.init(allocator, config, pool_config);
    defer pool.deinit();

    // Acquire a connection
    var client = try pool.acquire();
    try client.query("SELECT 1");
    
    // Release the connection back to the pool
    pool.release(client);

    std.debug.print("Pool example completed successfully\n", .{});
}