const std = @import("std");
const ClickHouseClient = @import("main.zig").ClickHouseClient;

pub const MutationType = enum {
    Delete,
    Update,
    Replace,
    Clear,
};

pub const MutationStatus = struct {
    is_done: bool,
    parts_to_do: u64,
    failed_part: ?[]const u8,
    fail_reason: ?[]const u8,
    elapsed_time_ms: u64,
    latest_fail_time: ?i64,
    mutation_id: []const u8,
};

pub const Mutation = struct {
    table: []const u8,
    type: MutationType,
    condition: ?[]const u8,
    settings: MutationSettings,
    allocator: std.mem.Allocator,
    cluster: ?[]const u8,
    values: ?std.StringHashMap([]const u8),

    pub const MutationSettings = struct {
        replication: bool = true,
        mutations_sync: bool = false,
        timeout_ms: u64 = 10000,
        retries: u32 = 3,
    };

    pub fn init(
        allocator: std.mem.Allocator,
        table: []const u8,
        mutation_type: MutationType,
    ) !*Mutation {
        var mutation = try allocator.create(Mutation);
        mutation.* = .{
            .table = try allocator.dupe(u8, table),
            .type = mutation_type,
            .condition = null,
            .settings = .{},
            .allocator = allocator,
            .cluster = null,
            .values = null,
        };
        return mutation;
    }

    pub fn deinit(self: *Mutation) void {
        self.allocator.free(self.table);
        if (self.condition) |cond| {
            self.allocator.free(cond);
        }
        if (self.cluster) |cluster| {
            self.allocator.free(cluster);
        }
        if (self.values) |*values| {
            var it = values.iterator();
            while (it.next()) |entry| {
                self.allocator.free(entry.key_ptr.*);
                self.allocator.free(entry.value_ptr.*);
            }
            values.deinit();
        }
        self.allocator.destroy(self);
    }

    pub fn setCondition(self: *Mutation, condition: []const u8) !void {
        if (self.condition) |old_condition| {
            self.allocator.free(old_condition);
        }
        self.condition = try self.allocator.dupe(u8, condition);
    }

    pub fn setCluster(self: *Mutation, cluster: []const u8) !void {
        if (self.cluster) |old_cluster| {
            self.allocator.free(old_cluster);
        }
        self.cluster = try self.allocator.dupe(u8, cluster);
    }

    pub fn setValue(self: *Mutation, column: []const u8, value: []const u8) !void {
        if (self.values == null) {
            self.values = std.StringHashMap([]const u8).init(self.allocator);
        }
        const key = try self.allocator.dupe(u8, column);
        const val = try self.allocator.dupe(u8, value);
        try self.values.?.put(key, val);
    }

    pub fn execute(self: *Mutation, client: *ClickHouseClient) ![]const u8 {
        var query_buf = std.ArrayList(u8).init(self.allocator);
        defer query_buf.deinit();

        try query_buf.writer().print("ALTER TABLE {s}", .{self.table});
        if (self.cluster) |cluster| {
            try query_buf.writer().print(" ON CLUSTER {s}", .{cluster});
        }

        switch (self.type) {
            .Delete => {
                try query_buf.appendSlice(" DELETE");
                if (self.condition) |cond| {
                    try query_buf.writer().print(" WHERE {s}", .{cond});
                }
            },
            .Update => {
                try query_buf.appendSlice(" UPDATE");
                if (self.values) |values| {
                    var it = values.iterator();
                    var first = true;
                    while (it.next()) |entry| {
                        if (!first) try query_buf.appendSlice(",");
                        try query_buf.writer().print(" {s} = {s}", .{ entry.key_ptr.*, entry.value_ptr.* });
                        first = false;
                    }
                }
                if (self.condition) |cond| {
                    try query_buf.writer().print(" WHERE {s}", .{cond});
                }
            },
            .Replace => {
                try query_buf.appendSlice(" REPLACE");
                if (self.values) |values| {
                    var it = values.iterator();
                    var first = true;
                    while (it.next()) |entry| {
                        if (!first) try query_buf.appendSlice(",");
                        try query_buf.writer().print(" {s} = {s}", .{ entry.key_ptr.*, entry.value_ptr.* });
                        first = false;
                    }
                }
                if (self.condition) |cond| {
                    try query_buf.writer().print(" WHERE {s}", .{cond});
                }
            },
            .Clear => {
                try query_buf.appendSlice(" CLEAR COLUMN");
                if (self.values) |values| {
                    var it = values.iterator();
                    var first = true;
                    while (it.next()) |entry| {
                        if (!first) try query_buf.appendSlice(",");
                        try query_buf.writer().print(" {s}", .{entry.key_ptr.*});
                        first = false;
                    }
                }
                if (self.condition) |cond| {
                    try query_buf.writer().print(" WHERE {s}", .{cond});
                }
            },
        }

        // Add settings
        try query_buf.appendSlice(" SETTINGS");
        try query_buf.writer().print(
            \\ mutations_sync = {d},
            \\ replication_alter_partitions_sync = {d},
            \\ mutations_timeout = {d}
        , .{
            @boolToInt(self.settings.mutations_sync),
            @boolToInt(self.settings.replication),
            self.settings.timeout_ms,
        });

        // Execute mutation
        try client.query(query_buf.items);

        // Return mutation ID for status tracking
        return try self.getMutationId(client);
    }

    fn getMutationId(self: *Mutation, client: *ClickHouseClient) ![]const u8 {
        var query_buf = std.ArrayList(u8).init(self.allocator);
        defer query_buf.deinit();

        try query_buf.writer().print(
            \\SELECT mutation_id
            \\FROM system.mutations
            \\WHERE table = '{s}'
            \\ORDER BY create_time DESC
            \\LIMIT 1
        , .{self.table});

        try client.query(query_buf.items);
        // Process result and return mutation ID
        return try self.allocator.dupe(u8, "mutation_id");
    }

    pub fn status(self: *Mutation, client: *ClickHouseClient, mutation_id: []const u8) !MutationStatus {
        var query_buf = std.ArrayList(u8).init(self.allocator);
        defer query_buf.deinit();

        try query_buf.writer().print(
            \\SELECT
            \\    is_done,
            \\    parts_to_do_count,
            \\    latest_failed_part,
            \\    latest_fail_reason,
            \\    elapsed,
            \\    latest_fail_time
            \\FROM system.mutations
            \\WHERE table = '{s}'
            \\  AND mutation_id = '{s}'
        , .{ self.table, mutation_id });

        try client.query(query_buf.items);

        // Process result and return status
        return MutationStatus{
            .is_done = false,
            .parts_to_do = 0,
            .failed_part = null,
            .fail_reason = null,
            .elapsed_time_ms = 0,
            .latest_fail_time = null,
            .mutation_id = try self.allocator.dupe(u8, mutation_id),
        };
    }
};