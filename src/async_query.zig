const std = @import("std");
const AsyncClient = @import("async_client.zig").AsyncClient;
const results = @import("results.zig");
const block = @import("block.zig");
const error = @import("error.zig");

pub const AsyncQuery = struct {
    client: *AsyncClient,
    query: []const u8,
    allocator: std.mem.Allocator,
    current_block: ?*block.Block,
    frame: @Frame(execute),
    callback: ?QueryCallback,

    pub const QueryCallback = fn (*AsyncQuery, ?*results.QueryResult, ?*error.Error) void;

    pub fn init(allocator: std.mem.Allocator, client: *AsyncClient, query: []const u8) AsyncQuery {
        return .{
            .client = client,
            .query = allocator.dupe(u8, query) catch unreachable,
            .allocator = allocator,
            .current_block = null,
            .frame = undefined,
            .callback = null,
        };
    }

    pub fn deinit(self: *AsyncQuery) void {
        if (self.current_block) |b| {
            b.deinit();
            self.allocator.destroy(b);
        }
        self.allocator.free(self.query);
    }

    pub fn execute(self: *AsyncQuery) !void {
        if (self.client.stream == null) {
            return error.ConnectionFailed;
        }

        self.frame = async self.executeInternal();
        try await self.frame;
    }

    pub fn executeWithCallback(self: *AsyncQuery, callback: QueryCallback) !void {
        self.callback = callback;
        try self.execute();
    }

    fn executeInternal(self: *AsyncQuery) !void {
        // Start query execution
        try self.client.queryAsync(self.query);

        // Process results asynchronously
        var result: ?*results.QueryResult = null;
        var err: ?*error.Error = null;

        if (self.current_block) |b| {
            result = try results.QueryResult.init(self.allocator, b);
        }

        // Call callback if set
        if (self.callback) |cb| {
            cb(self, result, err);
        }
    }

    pub fn cancel(self: *AsyncQuery) void {
        // Cancel the ongoing query
        if (self.frame) |frame| {
            frame.cancel();
        }
    }

    pub const AsyncIterator = struct {
        query: *AsyncQuery,
        current_row: usize,

        pub fn next(self: *AsyncIterator) !?results.Row {
            if (self.query.current_block == null or 
                self.current_row >= self.query.current_block.?.rows) {
                // Fetch next block asynchronously
                if (!try self.query.fetchNextBlockAsync()) {
                    return null;
                }
                self.current_row = 0;
            }

            const row = try results.Row.fromBlock(
                self.query.allocator,
                self.query.current_block.?,
                self.current_row
            );
            self.current_row += 1;
            return row;
        }
    };

    pub fn iterator(self: *AsyncQuery) AsyncIterator {
        return .{
            .query = self,
            .current_row = 0,
        };
    }

    fn fetchNextBlockAsync(self: *AsyncQuery) !bool {
        // This would be implemented to fetch the next block asynchronously
        // For now, return false to indicate no more blocks
        return false;
    }
};