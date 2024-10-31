const std = @import("std");
const ClickHouseClient = @import("../src/main.zig").ClickHouseClient;
const ClickHouseConfig = @import("../src/main.zig").ClickHouseConfig;
const QueryControl = @import("../src/query_control.zig").QueryControl;

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

    // Create query control with timeout
    var ctrl = try QueryControl.init(&client, allocator, 5000);
    defer ctrl.deinit();

    // Start a long-running query in a separate thread
    const thread = try std.Thread.spawn(.{}, struct {
        fn run(c: *ClickHouseClient) !void {
            try c.query(
                \\SELECT
                \\    number,
                \\    sleep(0.1)
                \\FROM system.numbers
                \\LIMIT 100
            );
        }
    }.run, .{&client});

    // Monitor query progress
    while (true) {
        if (ctrl.isTimeout()) {
            std.debug.print("Query timeout, cancelling...\n", .{});
            try ctrl.cancel();
            break;
        }

        const progress = try ctrl.getProgress();
        std.debug.print(
            "Progress: {d:.2}% ({} rows, {} bytes)\n",
            .{
                progress.progress * 100,
                progress.read_rows,
                progress.read_bytes,
            },
        );

        if (progress.progress >= 1.0) break;
        std.time.sleep(100 * std.time.ns_per_ms);
    }

    thread.join();
    std.debug.print("Query control example completed\n", .{});
}