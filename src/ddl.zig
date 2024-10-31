const std = @import("std");
const ClickHouseClient = @import("main.zig").ClickHouseClient;

pub const MaterializedView = struct {
    name: []const u8,
    target_table: []const u8,
    query: []const u8,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, name: []const u8, target_table: []const u8, query: []const u8) !MaterializedView {
        return MaterializedView{
            .name = try allocator.dupe(u8, name),
            .target_table = try allocator.dupe(u8, target_table),
            .query = try allocator.dupe(u8, query),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *MaterializedView) void {
        self.allocator.free(self.name);
        self.allocator.free(self.target_table);
        self.allocator.free(self.query);
    }

    pub fn create(self: *MaterializedView, client: *ClickHouseClient) !void {
        var query_buf = std.ArrayList(u8).init(self.allocator);
        defer query_buf.deinit();

        try query_buf.writer().print(
            \\CREATE MATERIALIZED VIEW {s}
            \\TO {s}
            \\AS {s}
        , .{ self.name, self.target_table, self.query });

        try client.query(query_buf.items);
    }

    pub fn drop(self: *MaterializedView, client: *ClickHouseClient) !void {
        var query_buf = std.ArrayList(u8).init(self.allocator);
        defer query_buf.deinit();

        try query_buf.writer().print("DROP VIEW IF EXISTS {s}", .{self.name});
        try client.query(query_buf.items);
    }
};

pub const Mutation = struct {
    table: []const u8,
    mutation: []const u8,
    allocator: std.mem.Allocator,
    settings: MutationSettings,

    pub const MutationSettings = struct {
        replication: bool = true,
        mutations_sync: bool = false,
    };

    pub fn init(allocator: std.mem.Allocator, table: []const u8, mutation: []const u8) Mutation {
        return .{
            .table = allocator.dupe(u8, table) catch unreachable,
            .mutation = allocator.dupe(u8, mutation) catch unreachable,
            .allocator = allocator,
            .settings = .{},
        };
    }

    pub fn deinit(self: *Mutation) void {
        self.allocator.free(self.table);
        self.allocator.free(self.mutation);
    }

    pub fn execute(self: *Mutation, client: *ClickHouseClient) !void {
        var query_buf = std.ArrayList(u8).init(self.allocator);
        defer query_buf.deinit();

        // Build ALTER TABLE query with mutation
        try query_buf.writer().print("ALTER TABLE {s} {s}", .{ self.table, self.mutation });

        // Add settings if needed
        if (!self.settings.replication) {
            try query_buf.appendSlice(" SETTINGS replication_alter_partitions_sync = 0");
        }
        if (self.settings.mutations_sync) {
            try query_buf.appendSlice(" SETTINGS mutations_sync = 1");
        }

        try client.query(query_buf.items);
    }

    pub fn status(self: *Mutation, client: *ClickHouseClient) !MutationStatus {
        var query_buf = std.ArrayList(u8).init(self.allocator);
        defer query_buf.deinit();

        try query_buf.writer().print(
            \\SELECT 
            \\    is_done,
            \\    latest_failed_part,
            \\    latest_fail_reason,
            \\    parts_to_do_count
            \\FROM system.mutations
            \\WHERE table = '{s}'
            \\ORDER BY create_time DESC
            \\LIMIT 1
        , .{self.table});

        try client.query(query_buf.items);
        // Process result and return status
        return MutationStatus{
            .is_done = false,
            .parts_to_do = 0,
            .failed_part = null,
            .fail_reason = null,
        };
    }
};

pub const MutationStatus = struct {
    is_done: bool,
    parts_to_do: u64,
    failed_part: ?[]const u8,
    fail_reason: ?[]const u8,
};

pub const TableOptions = struct {
    engine: Engine = .MergeTree,
    order_by: ?[]const u8 = null,
    partition_by: ?[]const u8 = null,
    primary_key: ?[]const u8 = null,
    sample_by: ?[]const u8 = null,
    settings: TableSettings = .{},

    pub const Engine = enum {
        MergeTree,
        ReplacingMergeTree,
        SummingMergeTree,
        AggregatingMergeTree,
        CollapsingMergeTree,
        VersionedCollapsingMergeTree,
        GraphiteMergeTree,
        Memory,
        Buffer,
    };

    pub const TableSettings = struct {
        index_granularity: u64 = 8192,
        enable_mixed_granularity_parts: bool = true,
        min_merge_bytes_to_use_direct_io: u64 = 10 * 1024 * 1024,
        merge_with_ttl_timeout: u64 = 3600,
    };
};

pub fn createTable(
    client: *ClickHouseClient,
    allocator: std.mem.Allocator,
    name: []const u8,
    columns: []const Column,
    options: TableOptions,
) !void {
    var query_buf = std.ArrayList(u8).init(allocator);
    defer query_buf.deinit();

    // Start CREATE TABLE query
    try query_buf.writer().print("CREATE TABLE {s} (\n", .{name});

    // Add columns
    for (columns, 0..) |col, i| {
        if (i > 0) try query_buf.appendSlice(",\n");
        try query_buf.writer().print("    {s} {s}", .{ col.name, col.type });
        if (col.codec) |codec| {
            try query_buf.writer().print(" CODEC({s})", .{codec});
        }
        if (col.ttl) |ttl| {
            try query_buf.writer().print(" TTL {s}", .{ttl});
        }
    }

    // Add engine and options
    try query_buf.appendSlice("\n) ENGINE = ");
    try query_buf.appendSlice(@tagName(options.engine));

    if (options.order_by) |order| {
        try query_buf.writer().print("\nORDER BY {s}", .{order});
    }

    if (options.partition_by) |partition| {
        try query_buf.writer().print("\nPARTITION BY {s}", .{partition});
    }

    if (options.primary_key) |pk| {
        try query_buf.writer().print("\nPRIMARY KEY {s}", .{pk});
    }

    if (options.sample_by) |sample| {
        try query_buf.writer().print("\nSAMPLE BY {s}", .{sample});
    }

    // Add settings
    try query_buf.appendSlice("\nSETTINGS");
    try query_buf.writer().print(
        \\
        \\ index_granularity = {d},
        \\ enable_mixed_granularity_parts = {d},
        \\ min_merge_bytes_to_use_direct_io = {d},
        \\ merge_with_ttl_timeout = {d}
    , .{
        options.settings.index_granularity,
        @boolToInt(options.settings.enable_mixed_granularity_parts),
        options.settings.min_merge_bytes_to_use_direct_io,
        options.settings.merge_with_ttl_timeout,
    });

    try client.query(query_buf.items);
}

pub const Column = struct {
    name: []const u8,
    type: []const u8,
    codec: ?[]const u8 = null,
    ttl: ?[]const u8 = null,
};