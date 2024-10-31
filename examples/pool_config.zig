const std = @import("std");
const ClickHouseClient = @import("../src/main.zig").ClickHouseClient;
const ClickHouseConfig = @import("../src/main.zig").ClickHouseConfig;
const Pool = @import("../src/pool.zig").Pool;
const PoolConfig = @import("../src/pool_config.zig").PoolConfig;
const RetryStrategy = @import("../src/retry.zig").RetryStrategy;

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

    // Configure connection pool
    var pool_config = PoolConfig.init();
    pool_config.setMinConnections(5);
    pool_config.setMaxConnections(20);
    pool_config.setConnectionTimeout(3000);
    pool_config.setMaxIdleTime(300000);
    pool_config.setHealthCheckInterval(30000);
    pool_config.setMaxWaitQueueSize(50);

    // Configure retry strategy
    var retry_strategy = RetryStrategy{
        .max_attempts = 3,
        .initial_delay_ms = 100,
        .max_delay_ms = 5000,
        .multiplier = 2.0,
        .jitter = true,
    };
    pool_config.setRetryStrategy(retry_strategy);

    // Configure TCP settings
    pool_config.setTcpKeepalive(true);
    pool_config.setTcpNodelay(true);

    // Create pool
    var pool = try Pool.init(allocator, config, pool_config);
    defer pool.deinit();

    // Acquire connection and execute query
    var client = try pool.acquire();
    try client.query("SELECT 1");
    pool.release(client);

    std.debug.print("Pool configuration example completed successfully\n", .{});
}