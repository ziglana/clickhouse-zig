const std = @import("std");
const ClickHouseClient = @import("main.zig").ClickHouseClient;

pub const QueryProfile = struct {
    query_id: []const u8,
    client: *ClickHouseClient,
    allocator: std.mem.Allocator,
    settings: ProfileSettings,

    pub const ProfileSettings = struct {
        collect_memory_stats: bool = true,
        collect_io_stats: bool = true,
        track_memory_reference: bool = false,
        max_threads: u32 = 0,
    };

    pub fn init(
        client: *ClickHouseClient,
        allocator: std.mem.Allocator,
        query_id: []const u8,
        settings: ProfileSettings,
    ) !*QueryProfile {
        var profile = try allocator.create(QueryProfile);
        profile.* = .{
            .query_id = try allocator.dupe(u8, query_id),
            .client = client,
            .allocator = allocator,
            .settings = settings,
        };
        return profile;
    }

    pub fn deinit(self: *QueryProfile) void {
        self.allocator.free(self.query_id);
        self.allocator.destroy(self);
    }

    pub fn getExecutionStats(self: *QueryProfile) !ExecutionStats {
        var query_buf = std.ArrayList(u8).init(self.allocator);
        defer query_buf.deinit();

        try query_buf.writer().print(
            \\SELECT
            \\    query_duration_ms,
            \\    read_rows,
            \\    read_bytes,
            \\    written_rows,
            \\    written_bytes,
            \\    memory_usage,
            \\    peak_memory_usage,
            \\    thread_count
            \\FROM system.query_log
            \\WHERE query_id = '{s}'
            \\  AND type = 'QueryFinish'
            \\ORDER BY event_time DESC
            \\LIMIT 1
        , .{self.query_id});

        try self.client.query(query_buf.items);

        return ExecutionStats{
            .duration_ms = 0,
            .read_rows = 0,
            .read_bytes = 0,
            .written_rows = 0,
            .written_bytes = 0,
            .memory_usage = 0,
            .peak_memory_usage = 0,
            .thread_count = 0,
        };
    }

    pub fn getThreadStats(self: *QueryProfile) ![]ThreadStat {
        var query_buf = std.ArrayList(u8).init(self.allocator);
        defer query_buf.deinit();

        try query_buf.writer().print(
            \\SELECT
            \\    thread_id,
            \\    thread_name,
            \\    thread_duration_ms,
            \\    thread_memory_usage,
            \\    thread_peak_memory_usage
            \\FROM system.query_thread_log
            \\WHERE query_id = '{s}'
            \\ORDER BY thread_id
        , .{self.query_id});

        try self.client.query(query_buf.items);

        return &[_]ThreadStat{};
    }
};

pub const ExecutionStats = struct {
    duration_ms: u64,
    read_rows: u64,
    read_bytes: u64,
    written_rows: u64,
    written_bytes: u64,
    memory_usage: u64,
    peak_memory_usage: u64,
    thread_count: u32,
};

pub const ThreadStat = struct {
    thread_id: u32,
    thread_name: []const u8,
    duration_ms: u64,
    memory_usage: u64,
    peak_memory_usage: u64,
};