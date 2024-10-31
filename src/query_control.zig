const std = @import("std");
const ClickHouseClient = @import("main.zig").ClickHouseClient;

pub const QueryControl = struct {
    client: *ClickHouseClient,
    query_id: []const u8,
    allocator: std.mem.Allocator,
    timeout_ms: ?u64,
    start_time: i64,
    is_cancelled: bool,

    pub fn init(client: *ClickHouseClient, allocator: std.mem.Allocator, timeout_ms: ?u64) !*QueryControl {
        var ctrl = try allocator.create(QueryControl);
        
        // Generate unique query ID
        var random_bytes: [16]u8 = undefined;
        try std.crypto.random.bytes(&random_bytes);
        const id = try std.fmt.allocPrint(allocator, "q_{}", .{
            std.fmt.fmtSliceHexLower(&random_bytes),
        });

        ctrl.* = .{
            .client = client,
            .query_id = id,
            .allocator = allocator,
            .timeout_ms = timeout_ms,
            .start_time = std.time.milliTimestamp(),
            .is_cancelled = false,
        };

        return ctrl;
    }

    pub fn cancel(self: *QueryControl) !void {
        if (self.is_cancelled) return;

        var query_buf = std.ArrayList(u8).init(self.allocator);
        defer query_buf.deinit();

        try query_buf.writer().print(
            \\KILL QUERY WHERE query_id = '{s}'
        , .{self.query_id});

        try self.client.query(query_buf.items);
        self.is_cancelled = true;
    }

    pub fn isTimeout(self: *QueryControl) bool {
        if (self.timeout_ms) |timeout| {
            const elapsed = std.time.milliTimestamp() - self.start_time;
            return elapsed >= timeout;
        }
        return false;
    }

    pub fn getProgress(self: *QueryControl) !QueryProgress {
        var query_buf = std.ArrayList(u8).init(self.allocator);
        defer query_buf.deinit();

        try query_buf.writer().print(
            \\SELECT 
            \\    read_rows,
            \\    read_bytes,
            \\    total_rows_to_read,
            \\    elapsed,
            \\    progress
            \\FROM system.processes
            \\WHERE query_id = '{s}'
        , .{self.query_id});

        try self.client.query(query_buf.items);

        return QueryProgress{
            .read_rows = 0,
            .read_bytes = 0,
            .total_rows = 0,
            .elapsed_ms = 0,
            .progress = 0,
        };
    }

    pub fn deinit(self: *QueryControl) void {
        if (!self.is_cancelled) {
            self.cancel() catch {};
        }
        self.allocator.free(self.query_id);
        self.allocator.destroy(self);
    }
};

pub const QueryProgress = struct {
    read_rows: u64,
    read_bytes: u64,
    total_rows: u64,
    elapsed_ms: u64,
    progress: f64,
};