const std = @import("std");
const ClickHouseClient = @import("../src/main.zig").ClickHouseClient;
const ClickHouseConfig = @import("../src/main.zig").ClickHouseConfig;
const RowStream = @import("../src/stream.zig").RowStream;
const StreamBuffer = @import("../src/stream_buffer.zig").StreamBuffer;

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
        \\CREATE TABLE IF NOT EXISTS test_stream (
        \\    id UInt32,
        \\    name String,
        \\    value Float64
        \\) ENGINE = MergeTree()
        \\ORDER BY id
    );

    // Insert sample data
    try client.query(
        \\INSERT INTO test_stream
        \\SELECT
        \\    number as id,
        \\    concat('name_', toString(number)) as name,
        \\    number * 1.5 as value
        \\FROM system.numbers
        \\LIMIT 1000000
    );

    // Create row stream with buffer
    var stream = RowStream.init(allocator, 10000);
    defer stream.deinit();

    // Create stream buffer
    var buffer = StreamBuffer.init(allocator);
    defer buffer.deinit();
    buffer.setCapacity(5000);

    // Start streaming query
    const query = "SELECT * FROM test_stream WHERE id % 100 = 0";
    var iterator = stream.iterator();

    // Process rows in chunks
    var row_count: usize = 0;
    while (try iterator.next()) |row| {
        // Buffer the row
        try buffer.push(row);
        row_count += 1;

        // Process buffered rows when buffer is full
        if (buffer.size >= buffer.capacity) {
            try processBufferedRows(&buffer);
        }
    }

    // Process any remaining rows
    if (buffer.size > 0) {
        try processBufferedRows(&buffer);
    }

    std.debug.print("Processed {} rows using streaming\n", .{row_count});
}

fn processBufferedRows(buffer: *StreamBuffer) !void {
    while (buffer.pop()) |row| {
        // Process each row
        const id = try row.getUInt32("id");
        const name = try row.getString("name");
        const value = try row.getFloat64("value");

        // Do something with the row data...
        _ = id;
        _ = name;
        _ = value;
    }
}