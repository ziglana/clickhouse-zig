const std = @import("std");
const ClickHouseClient = @import("../src/main.zig").ClickHouseClient;
const ClickHouseConfig = @import("../src/main.zig").ClickHouseConfig;
const MaterializedView = @import("../src/views.zig").MaterializedView;

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

    // Create source table
    try client.query(
        \\CREATE TABLE IF NOT EXISTS events (
        \\    timestamp DateTime,
        \\    user_id UInt32,
        \\    event_type String,
        \\    value Float64
        \\) ENGINE = MergeTree()
        \\ORDER BY (timestamp, user_id)
    );

    // Create materialized view
    var view = try MaterializedView.init(
        allocator,
        "events_hourly",
        "events_hourly_table",
        \\SELECT
        \\    toStartOfHour(timestamp) as hour,
        \\    user_id,
        \\    event_type,
        \\    count() as event_count,
        \\    sum(value) as total_value
        \\FROM events
        \\GROUP BY hour, user_id, event_type
    );
    defer view.deinit();

    // Create the view
    try view.create(&client);

    std.debug.print("Materialized view created successfully\n", .{});
}