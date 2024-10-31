const std = @import("std");
const ClickHouseClient = @import("../src/main.zig").ClickHouseClient;
const ClickHouseConfig = @import("../src/main.zig").ClickHouseConfig;
const QueryContext = @import("../src/query.zig").QueryContext;

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

    // Create query context
    var ctx = try QueryContext.init(allocator);
    defer ctx.deinit();

    // Start a long-running query
    const query = "SELECT sleep(10)";
    
    // Spawn a thread to cancel the query after 2 seconds
    const thread = try std.Thread.spawn(.{}, struct {
        fn run(token: *QueryContext) void {
            std.time.sleep(2 * std.time.ns_per_s);
            token.cancel_token.cancel();
        }
    }.run, .{ctx});

    // Execute query with cancellation context
    client.queryWithContext(query, ctx) catch |err| {
        if (err == error.QueryCancelled) {
            std.debug.print("Query was cancelled as expected\n", .{});
        } else {
            return err;
        }
    };

    thread.join();
}