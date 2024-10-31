const std = @import("std");
const ClickHouseClient = @import("main.zig").ClickHouseClient;

pub const Transaction = struct {
    client: *ClickHouseClient,
    id: []const u8,
    allocator: std.mem.Allocator,
    is_active: bool,

    pub fn begin(client: *ClickHouseClient, allocator: std.mem.Allocator) !*Transaction {
        var tx = try allocator.create(Transaction);
        
        // Generate unique transaction ID
        var random_bytes: [16]u8 = undefined;
        try std.crypto.random.bytes(&random_bytes);
        const tx_id = try std.fmt.allocPrint(allocator, "tx_{}", .{
            std.fmt.fmtSliceHexLower(&random_bytes),
        });

        tx.* = .{
            .client = client,
            .id = tx_id,
            .allocator = allocator,
            .is_active = true,
        };

        // Start transaction
        try client.query("BEGIN TRANSACTION");
        
        return tx;
    }

    pub fn commit(self: *Transaction) !void {
        if (!self.is_active) return error.TransactionNotActive;
        
        try self.client.query("COMMIT");
        self.is_active = false;
    }

    pub fn rollback(self: *Transaction) !void {
        if (!self.is_active) return error.TransactionNotActive;
        
        try self.client.query("ROLLBACK");
        self.is_active = false;
    }

    pub fn query(self: *Transaction, sql: []const u8) !void {
        if (!self.is_active) return error.TransactionNotActive;
        
        // Execute query within transaction
        try self.client.query(sql);
    }

    pub fn deinit(self: *Transaction) void {
        if (self.is_active) {
            self.rollback() catch {};
        }
        self.allocator.free(self.id);
        self.allocator.destroy(self);
    }
};

pub const TransactionOptions = struct {
    isolation_level: IsolationLevel = .ReadCommitted,
    read_only: bool = false,
    timeout_ms: u64 = 30000,

    pub const IsolationLevel = enum {
        ReadUncommitted,
        ReadCommitted,
        RepeatableRead,
        Serializable,
    };
};