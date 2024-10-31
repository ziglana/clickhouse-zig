const std = @import("std");
const types = @import("types.zig");
const block = @import("block.zig");

pub const BulkInsert = struct {
    allocator: std.mem.Allocator,
    table: []const u8,
    columns: []Column,
    batch_size: usize,
    current_row: usize,
    
    pub const Column = struct {
        name: []const u8,
        type_info: types.TypeInfo,
        data: std.ArrayList(u8),
    };

    pub fn init(allocator: std.mem.Allocator, table: []const u8, column_defs: []const ColumnDef, batch_size: usize) !BulkInsert {
        var columns = try allocator.alloc(Column, column_defs.len);
        
        for (column_defs, 0..) |def, i| {
            columns[i] = .{
                .name = try allocator.dupe(u8, def.name),
                .type_info = try types.TypeInfo.parse(allocator, def.type_str),
                .data = std.ArrayList(u8).init(allocator),
            };
        }

        return BulkInsert{
            .allocator = allocator,
            .table = try allocator.dupe(u8, table),
            .columns = columns,
            .batch_size = batch_size,
            .current_row = 0,
        };
    }

    pub fn deinit(self: *BulkInsert) void {
        for (self.columns) |*column| {
            self.allocator.free(column.name);
            column.data.deinit();
        }
        self.allocator.free(self.columns);
        self.allocator.free(self.table);
    }

    pub fn addRow(self: *BulkInsert, values: []const Value) !bool {
        if (values.len != self.columns.len) return error.ColumnCountMismatch;

        for (values, 0..) |value, i| {
            try self.addValue(&self.columns[i], value);
        }

        self.current_row += 1;
        return self.current_row >= self.batch_size;
    }

    fn addValue(self: *BulkInsert, column: *Column, value: Value) !void {
        switch (value) {
            .Int8 => |v| try column.data.writer().writeIntLittle(i8, v),
            .Int16 => |v| try column.data.writer().writeIntLittle(i16, v),
            .Int32 => |v| try column.data.writer().writeIntLittle(i32, v),
            .Int64 => |v| try column.data.writer().writeIntLittle(i64, v),
            .UInt8 => |v| try column.data.writer().writeIntLittle(u8, v),
            .UInt16 => |v| try column.data.writer().writeIntLittle(u16, v),
            .UInt32 => |v| try column.data.writer().writeIntLittle(u32, v),
            .UInt64 => |v| try column.data.writer().writeIntLittle(u64, v),
            .Float32 => |v| try column.data.writer().writeIntLittle(f32, v),
            .Float64 => |v| try column.data.writer().writeIntLittle(f64, v),
            .String => |v| {
                try column.data.writer().writeIntLittle(u64, v.len);
                try column.data.appendSlice(v);
            },
            .DateTime => |v| try column.data.writer().writeIntLittle(i64, v),
            .Date => |v| try column.data.writer().writeIntLittle(i32, v),
            .UUID => |v| try column.data.appendSlice(&v),
            .Array => |v| try self.addArray(column, v),
            .Null => if (column.type_info.nullable) {
                try column.data.append(1);
            } else return error.NullValueForNonNullableColumn,
        }
    }

    fn addArray(self: *BulkInsert, column: *Column, values: []const Value) !void {
        try column.data.writer().writeIntLittle(u64, values.len);
        for (values) |value| {
            try self.addValue(column, value);
        }
    }

    pub fn createBlock(self: *BulkInsert) !block.Block {
        var result = block.Block.init(self.allocator);
        
        for (self.columns) |column| {
            try result.addColumn(column.name, @tagName(column.type_info.base_type));
            const col_idx = result.columns.len - 1;
            result.columns[col_idx].data = try self.allocator.dupe(u8, column.data.items);
        }
        
        result.rows = self.current_row;
        return result;
    }

    pub fn reset(self: *BulkInsert) void {
        for (self.columns) |*column| {
            column.data.clearRetainingCapacity();
        }
        self.current_row = 0;
    }
};

pub const ColumnDef = struct {
    name: []const u8,
    type_str: []const u8,
};