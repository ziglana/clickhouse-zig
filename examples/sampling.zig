const std = @import("std");
const ClickHouseClient = @import("../src/main.zig").ClickHouseClient;
const ClickHouseConfig = @import("../src/main.zig").ClickHouseConfig;
const sampling = @import("../src/sampling.zig");

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

    // Create test table with sampling
    try client.query(
        \\CREATE TABLE IF NOT EXISTS test_sampling (
        \\    id UInt32,
        \\    name String,
        \\    value Float64
        \\) ENGINE = MergeTree()
        \\ORDER BY id
        \\SAMPLE BY cityHash64(id)
    );

    // Insert sample data
    try client.query(
        \\INSERT INTO test_sampling
        \\SELECT 
        \\    number as id,
        \\    concat('name_', toString(number)) as name,
        \\    number * 1.5 as value
        \\FROM system.numbers
        \\LIMIT 1000000
    );

    // Random sampling
    const random_sample = try sampling.applySampling(
        &client,
        "SELECT * FROM test_sampling",
        .{
            .method = .Random,
            .sample_size = 0.1,
            .seed = 42,
        },
        allocator,
    );
    defer allocator.free(random_sample);
    try client.query(random_sample);

    // Deterministic sampling
    const deterministic_sample = try sampling.applySampling(
        &client,
        "SELECT * FROM test_sampling",
        .{
            .method = .Deterministic,
            .sample_size = 0.1,
        },
        allocator,
    );
    defer allocator.free(deterministic_sample);
    try client.query(deterministic_sample);

    // Stratified sampling
    var stratified = try sampling.StratifiedSampling.init(allocator, &[_]sampling.StratifiedSampling.Stratum{
        .{
            .column = "value",
            .value = "< 1000",
            .sample_size = 1000,
        },
        .{
            .column = "value",
            .value = "BETWEEN 1000 AND 10000",
            .sample_size = 500,
        },
        .{
            .column = "value",
            .value = "> 10000",
            .sample_size = 100,
        },
    });
    defer stratified.deinit();

    try stratified.apply(&client, "test_sampling");

    std.debug.print("Sampling examples completed successfully\n", .{});
}