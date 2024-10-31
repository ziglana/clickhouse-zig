const std = @import("std");

pub const QueryState = enum {
    Running,
    Cancelled,
    Completed,
    Error,
};

pub const QueryContext = struct {
    id: []const u8,
    state: QueryState,
    allocator: std.mem.Allocator,
    cancel_token: CancelToken,

    pub fn init(allocator: std.mem.Allocator) !*QueryContext {
        var ctx = try allocator.create(QueryContext);
        const random_bytes = try std.crypto.random.bytes(allocator, 16);
        const id = try std.fmt.allocPrint(allocator, "{}", .{std.fmt.fmtSliceHexLower(random_bytes)});
        
        ctx.* = .{
            .id = id,
            .state = .Running,
            .allocator = allocator,
            .cancel_token = CancelToken.init(),
        };
        return ctx;
    }

    pub fn deinit(self: *QueryContext) void {
        self.allocator.free(self.id);
        self.allocator.destroy(self);
    }
};

pub const CancelToken = struct {
    cancelled: std.atomic.Atomic(bool),

    pub fn init() CancelToken {
        return .{
            .cancelled = std.atomic.Atomic(bool).init(false),
        };
    }

    pub fn cancel(self: *CancelToken) void {
        self.cancelled.store(true, .Release);
    }

    pub fn isCancelled(self: *CancelToken) bool {
        return self.cancelled.load(.Acquire);
    }
};