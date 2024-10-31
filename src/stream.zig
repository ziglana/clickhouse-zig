const std = @import("std");
const block = @import("block.zig");
const results = @import("results.zig");

pub const RowStream = struct {
    allocator: std.mem.Allocator,
    current_block: ?*block.Block,
    current_row: usize,
    total_rows: usize,
    buffer_size: usize,
    
    pub fn init(allocator: std.mem.Allocator, buffer_size: usize) RowStream {
        return .{
            .allocator = allocator,
            .current_block = null,
            .current_row = 0,
            .total_rows = 0,
            .buffer_size = buffer_size,
        };
    }

    pub fn deinit(self: *RowStream) void {
        if (self.current_block) |b| {
            b.deinit();
            self.allocator.destroy(b);
            self.current_block = null;
        }
    }

    pub fn next(self: *RowStream) !?results.Row {
        if (self.current_block == null or self.current_row >= self.current_block.?.rows) {
            if (!try self.fetchNextBlock()) {
                return null;
            }
        }

        const row = try results.Row.fromBlock(self.allocator, self.current_block.?, self.current_row);
        self.current_row += 1;
        return row;
    }

    pub fn fetchNextBlock(self: *RowStream) !bool {
        // This would be implemented by the client to fetch the next block of data
        // For now, return false to indicate no more blocks
        return false;
    }

    pub const Iterator = struct {
        stream: *RowStream,
        
        pub fn next(self: *Iterator) !?results.Row {
            return self.stream.next();
        }
    };

    pub fn iterator(self: *RowStream) Iterator {
        return .{ .stream = self };
    }
};