const std = @import("std");
const ClickHouseClient = @import("../src/main.zig").ClickHouseClient;
const ClickHouseConfig = @import("../src/main.zig").ClickHouseConfig;
const DistributedTable = @import("../src/distributed.zig").DistributedTable;

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

    // Create local table first
    try client.query(
        \\CREATE TABLE IF NOT EXISTS events_local (
        \\    event_time DateTime,
        \\    user_id UInt32,
        \\    event_type String,
        \\    value Float64
        \\) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/events', '{replica}')
        \\PARTITION BY toYYYYMM(event_time)
        \\ORDER BY (event_time, user_id)
    );

    // Create distributed table
    var dist_table = try DistributedTable.init(
        allocator,
        "events_distributed",
        "my_cluster",
        "default",
        "events_local"
    );
    defer dist_table.deinit();

    // Set sharding key
    try dist_table.setShardingKey("cityHash64(user_id)");

    // Create the distributed table
    try dist_table.create(&client);

    std.debug.print("Distributed table created successfully\n", .{});
}