const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("types.zig");

pub const Column = struct {
    name: []const u8,
    type: types.ClickHouseType,
    data: []const u8,
    is_nullable: bool,
    
    pub fn init(allocator: Allocator, name: []const u8, type_str: []const u8) !Column {
        const ch_type = try types.ClickHouseType.fromStr(type_str);
        return Column{
            .name = try allocator.dupe(u8, name),
            .type = ch_type,
            .data = &[_]u8{},
            .is_nullable = std.mem.startsWith(u8, type_str, "Nullable"),
        };
    }
};

pub const Block = struct {
    columns: []Column,
    rows: usize,
    allocator: Allocator,

    pub fn init(allocator: Allocator) Block {
        return Block{
            .columns = &[_]Column{},
            .rows = 0,
            .allocator = allocator,
        };
    }

    pub fn addColumn(self: *Block, name: []const u8, type_str: []const u8) !void {
        const column = try Column.init(self.allocator, name, type_str);
        const new_columns = try self.allocator.realloc(self.columns, self.columns.len + 1);
        new_columns[new_columns.len - 1] = column;
        self.columns = new_columns;
    }

    pub fn deinit(self: *Block) void {
        for (self.columns) |column| {
            self.allocator.free(column.name);
            self.allocator.free(column.data);
        }
        self.allocator.free(self.columns);
    }
};