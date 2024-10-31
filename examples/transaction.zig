const std = @import("std");
const ClickHouseClient = @import("../src/main.zig").ClickHouseClient;
const ClickHouseConfig = @import("../src/main.zig").ClickHouseConfig;
const Transaction = @import("../src/transaction.zig").Transaction;

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

    // Start transaction
    var tx = try Transaction.begin(&client, allocator);
    defer tx.deinit();

    // Execute queries within transaction
    tx.query(
        \\INSERT INTO test_table (id, name)
        \\VALUES (1, 'test1')
    ) catch |err| {
        try tx.rollback();
        return err;
    };

    tx.query(
        \\INSERT INTO test_table (id, name)
        \\VALUES (2, 'test2')
    ) catch |err| {
        try tx.rollback();
        return err;
    };

    // Commit transaction
    try tx.commit();

    std.debug.print("Transaction completed successfully\n", .{});
}