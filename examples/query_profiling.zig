const std = @import("std");
const ClickHouseClient = @import("../src/main.zig").ClickHouseClient;
const ClickHouseConfig = @import("../src/main.zig").ClickHouseConfig;
const QueryProfile = @import("../src/profiling.zig").QueryProfile;

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

    // Create a query profile
    var profile = try QueryProfile.init(
        &client,
        allocator,
        "test_query",
        .{
            .collect_memory_stats = true,
            .collect_io_stats = true,
            .track_memory_reference = true,
        }
    );
    defer profile.deinit();

    // Execute a test query
    try client.query(
        \\SELECT 
        \\    number,
        \\    toString(number) as str,
        \\    number * 2 as double
        \\FROM system.numbers
        \\LIMIT 1000000
    );

    // Get execution statistics
    const stats = try profile.getExecutionStats();
    std.debug.print("\nQuery Statistics:\n", .{});
    std.debug.print("Duration: {}ms\n", .{stats.duration_ms});
    std.debug.print("Rows Read: {}\n", .{stats.read_rows});
    std.debug.print("Bytes Read: {}\n", .{stats.read_bytes});
    std.debug.print("Memory Usage: {}\n", .{stats.memory_usage});
    std.debug.print("Peak Memory: {}\n", .{stats.peak_memory_usage});
    std.debug.print("Thread Count: {}\n", .{stats.thread_count});

    // Get thread statistics
    const thread_stats = try profile.getThreadStats();
    std.debug.print("\nThread Statistics:\n", .{});
    for (thread_stats) |thread| {
        std.debug.print("Thread {}: {}ms, Memory: {}\n", .{
            thread.thread_id,
            thread.duration_ms,
            thread.memory_usage,
        });
    }
}