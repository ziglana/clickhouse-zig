const std = @import("std");
const ClickHouseClient = @import("../src/main.zig").ClickHouseClient;
const ClickHouseConfig = @import("../src/main.zig").ClickHouseConfig;
const Mutation = @import("../src/mutations.zig").Mutation;

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

    // Create test table
    try client.query(
        \\CREATE TABLE IF NOT EXISTS test_mutations (
        \\    id UInt32,
        \\    name String,
        \\    value Float64,
        \\    status String
        \\) ENGINE = MergeTree()
        \\ORDER BY id
    );

    // Create mutation
    var mutation = try Mutation.init(allocator, "test_mutations", .Update);
    defer mutation.deinit();

    // Set mutation parameters
    try mutation.setCondition("status = 'pending'");
    try mutation.setValue("status", "'completed'");
    try mutation.setValue("value", "value * 2");

    // Execute mutation
    const mutation_id = try mutation.execute(&client);

    // Monitor mutation status
    while (true) {
        const status = try mutation.status(&client, mutation_id);
        if (status.is_done) {
            std.debug.print("Mutation completed successfully\n", .{});
            break;
        }
        if (status.failed_part != null) {
            std.debug.print("Mutation failed: {s}\n", .{status.fail_reason.?});
            break;
        }
        std.debug.print("Mutation in progress: {} parts remaining\n", .{status.parts_to_do});
        std.time.sleep(1 * std.time.ns_per_s);
    }
}