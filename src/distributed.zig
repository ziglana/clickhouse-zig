const std = @import("std");
const ClickHouseClient = @import("main.zig").ClickHouseClient;

pub const DistributedTable = struct {
    name: []const u8,
    cluster: []const u8,
    database: []const u8,
    local_table: []const u8,
    sharding_key: ?[]const u8,
    allocator: std.mem.Allocator,

    pub fn init(
        allocator: std.mem.Allocator,
        name: []const u8,
        cluster: []const u8,
        database: []const u8,
        local_table: []const u8,
    ) !DistributedTable {
        return DistributedTable{
            .name = try allocator.dupe(u8, name),
            .cluster = try allocator.dupe(u8, cluster),
            .database = try allocator.dupe(u8, database),
            .local_table = try allocator.dupe(u8, local_table),
            .sharding_key = null,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *DistributedTable) void {
        self.allocator.free(self.name);
        self.allocator.free(self.cluster);
        self.allocator.free(self.database);
        self.allocator.free(self.local_table);
        if (self.sharding_key) |key| {
            self.allocator.free(key);
        }
    }

    pub fn create(self: *DistributedTable, client: *ClickHouseClient) !void {
        var query_buf = std.ArrayList(u8).init(self.allocator);
        defer query_buf.deinit();

        try query_buf.writer().print(
            \\CREATE TABLE {s} AS {s}.{s}
            \\ENGINE = Distributed(
            \\    {s},
            \\    {s},
            \\    {s}
        , .{
            self.name,
            self.database,
            self.local_table,
            self.cluster,
            self.database,
            self.local_table,
        });

        if (self.sharding_key) |key| {
            try query_buf.writer().print(",\n    {s}", .{key});
        }

        try query_buf.appendSlice("\n)");
        try client.query(query_buf.items);
    }

    pub fn setShardingKey(self: *DistributedTable, key: []const u8) !void {
        if (self.sharding_key) |old_key| {
            self.allocator.free(old_key);
        }
        self.sharding_key = try self.allocator.dupe(u8, key);
    }
};

pub const ReplicatedTable = struct {
    name: []const u8,
    zk_path: []const u8,
    replica_name: []const u8,
    engine: Engine,
    allocator: std.mem.Allocator,

    pub const Engine = enum {
        ReplicatedMergeTree,
        ReplicatedReplacingMergeTree,
        ReplicatedSummingMergeTree,
        ReplicatedAggregatingMergeTree,
        ReplicatedCollapsingMergeTree,
        ReplicatedVersionedCollapsingMergeTree,
    };

    pub fn init(
        allocator: std.mem.Allocator,
        name: []const u8,
        zk_path: []const u8,
        replica_name: []const u8,
        engine: Engine,
    ) !ReplicatedTable {
        return ReplicatedTable{
            .name = try allocator.dupe(u8, name),
            .zk_path = try allocator.dupe(u8, zk_path),
            .replica_name = try allocator.dupe(u8, replica_name),
            .engine = engine,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *ReplicatedTable) void {
        self.allocator.free(self.name);
        self.allocator.free(self.zk_path);
        self.allocator.free(self.replica_name);
    }

    pub fn create(self: *ReplicatedTable, client: *ClickHouseClient, columns: []const Column) !void {
        var query_buf = std.ArrayList(u8).init(self.allocator);
        defer query_buf.deinit();

        try query_buf.writer().print("CREATE TABLE {s} (\n", .{self.name});

        // Add columns
        for (columns, 0..) |col, i| {
            if (i > 0) try query_buf.appendSlice(",\n");
            try query_buf.writer().print("    {s} {s}", .{ col.name, col.type });
        }

        // Add engine
        try query_buf.writer().print(
            \\
            \\) ENGINE = {s}(
            \\    '{s}',
            \\    '{s}'
            \\)
        , .{ @tagName(self.engine), self.zk_path, self.replica_name });

        try client.query(query_buf.items);
    }
};

pub const Column = struct {
    name: []const u8,
    type: []const u8,
};