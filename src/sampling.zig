const std = @import("std");
const ClickHouseClient = @import("main.zig").ClickHouseClient;

pub const SamplingMethod = enum {
    Random,
    Deterministic,
};

pub const SamplingOptions = struct {
    method: SamplingMethod = .Deterministic,
    sample_size: f64,
    seed: ?u64 = null,
};

pub fn applySampling(
    client: *ClickHouseClient,
    query: []const u8,
    options: SamplingOptions,
    allocator: std.mem.Allocator,
) ![]const u8 {
    var query_buf = std.ArrayList(u8).init(allocator);
    defer query_buf.deinit();

    // Find the FROM clause
    const from_pos = std.mem.indexOf(u8, query, "FROM") orelse
        return error.InvalidQuery;

    // Insert the query up to FROM
    try query_buf.appendSlice(query[0..from_pos]);

    // Add sampling clause
    try query_buf.appendSlice("FROM ");
    
    // Add table name and sampling
    const remaining = query[from_pos + 4 ..];
    const space_pos = std.mem.indexOf(u8, remaining, " ") orelse remaining.len;
    const table = remaining[0..space_pos];
    
    try query_buf.appendSlice(table);
    
    switch (options.method) {
        .Random => {
            try query_buf.writer().print(
                " SAMPLE {d}",
                .{options.sample_size},
            );
            if (options.seed) |seed| {
                try query_buf.writer().print(" SEED {d}", .{seed});
            }
        },
        .Deterministic => {
            try query_buf.writer().print(
                " SAMPLE {d} OFFSET {d}",
                .{ options.sample_size, if (options.seed) |s| s else 0 },
            );
        },
    }

    // Add the rest of the query
    if (space_pos < remaining.len) {
        try query_buf.appendSlice(remaining[space_pos..]);
    }

    return query_buf.toOwnedSlice();
}

pub fn estimateSampleSize(
    client: *ClickHouseClient,
    table: []const u8,
    confidence_level: f64,
    margin_of_error: f64,
    allocator: std.mem.Allocator,
) !usize {
    var query_buf = std.ArrayList(u8).init(allocator);
    defer query_buf.deinit();

    // Get total count
    try query_buf.writer().print("SELECT count() FROM {s}", .{table});
    try client.query(query_buf.items);
    
    // Calculate sample size using Cochran's formula
    // For now, return a reasonable default
    return 1000;
}

pub const StratifiedSampling = struct {
    strata: []const Stratum,
    allocator: std.mem.Allocator,

    pub const Stratum = struct {
        column: []const u8,
        value: []const u8,
        sample_size: usize,
    };

    pub fn init(allocator: std.mem.Allocator, strata: []const Stratum) !StratifiedSampling {
        return StratifiedSampling{
            .strata = try allocator.dupe(Stratum, strata),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *StratifiedSampling) void {
        self.allocator.free(self.strata);
    }

    pub fn apply(
        self: *StratifiedSampling,
        client: *ClickHouseClient,
        table: []const u8,
    ) !void {
        var query_buf = std.ArrayList(u8).init(self.allocator);
        defer query_buf.deinit();

        // Create temporary tables for each stratum
        for (self.strata) |stratum| {
            try query_buf.writer().print(
                \\CREATE TEMPORARY TABLE tmp_{s}_{s} AS
                \\SELECT *
                \\FROM {s}
                \\WHERE {s} = '{s}'
                \\SAMPLE {d}
            , .{
                table,
                stratum.column,
                table,
                stratum.column,
                stratum.value,
                stratum.sample_size,
            });

            try client.query(query_buf.items);
            query_buf.clearRetainingCapacity();
        }

        // Union all strata
        try query_buf.writer().print(
            \\CREATE TEMPORARY TABLE {s}_stratified AS
            \\SELECT * FROM (
        , .{table});

        for (self.strata, 0..) |stratum, i| {
            if (i > 0) try query_buf.appendSlice("\nUNION ALL\n");
            try query_buf.writer().print(
                "SELECT * FROM tmp_{s}_{s}",
                .{ table, stratum.column },
            );
        }

        try query_buf.appendSlice(")");
        try client.query(query_buf.items);
    }
};