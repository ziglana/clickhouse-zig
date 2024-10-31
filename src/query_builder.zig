const std = @import("std");
const types = @import("types.zig");

pub const QueryBuilder = struct {
    allocator: std.mem.Allocator,
    buffer: std.ArrayList(u8),
    params: std.ArrayList(Parameter),
    
    pub const Parameter = struct {
        type_info: types.TypeInfo,
        value: Value,
    };

    pub const Value = union(enum) {
        Int8: i8,
        Int16: i16,
        Int32: i32,
        Int64: i64,
        UInt8: u8,
        UInt16: u16,
        UInt32: u32,
        UInt64: u64,
        Float32: f32,
        Float64: f64,
        String: []const u8,
        DateTime: i64,
        Date: i32,
        UUID: [16]u8,
        Array: []const Value,
        Null,
    };

    pub fn init(allocator: std.mem.Allocator) QueryBuilder {
        return .{
            .allocator = allocator,
            .buffer = std.ArrayList(u8).init(allocator),
            .params = std.ArrayList(Parameter).init(allocator),
        };
    }

    pub fn deinit(self: *QueryBuilder) void {
        self.buffer.deinit();
        self.params.deinit();
    }

    pub fn reset(self: *QueryBuilder) void {
        self.buffer.clearRetainingCapacity();
        self.params.clearRetainingCapacity();
    }

    pub fn append(self: *QueryBuilder, sql: []const u8) !void {
        try self.buffer.appendSlice(sql);
    }

    pub fn param(self: *QueryBuilder, value: anytype) !void {
        const T = @TypeOf(value);
        const type_info = try self.inferTypeInfo(T);
        const param_value = try self.convertToValue(value);
        
        try self.params.append(.{
            .type_info = type_info,
            .value = param_value,
        });
        
        try self.buffer.appendSlice("?");
    }

    fn inferTypeInfo(self: *QueryBuilder, comptime T: type) !types.TypeInfo {
        return switch (@typeInfo(T)) {
            .Int => |info| switch (info.bits) {
                8 => .{ .base_type = if (info.signedness == .signed) .Int8 else .UInt8 },
                16 => .{ .base_type = if (info.signedness == .signed) .Int16 else .UInt16 },
                32 => .{ .base_type = if (info.signedness == .signed) .Int32 else .UInt32 },
                64 => .{ .base_type = if (info.signedness == .signed) .Int64 else .UInt64 },
                else => error.UnsupportedType,
            },
            .Float => |info| switch (info.bits) {
                32 => .{ .base_type = .Float32 },
                64 => .{ .base_type = .Float64 },
                else => error.UnsupportedType,
            },
            .Array => |info| if (info.child == u8) .{
                .base_type = .String,
            } else .{
                .base_type = .Array,
                .array_level = 1,
            },
            else => error.UnsupportedType,
        };
    }

    fn convertToValue(self: *QueryBuilder, value: anytype) !Value {
        const T = @TypeOf(value);
        return switch (@typeInfo(T)) {
            .Int => |info| switch (info.bits) {
                8 => if (info.signedness == .signed) .{ .Int8 = value } else .{ .UInt8 = value },
                16 => if (info.signedness == .signed) .{ .Int16 = value } else .{ .UInt16 = value },
                32 => if (info.signedness == .signed) .{ .Int32 = value } else .{ .UInt32 = value },
                64 => if (info.signedness == .signed) .{ .Int64 = value } else .{ .UInt64 = value },
                else => error.UnsupportedType,
            },
            .Float => |info| switch (info.bits) {
                32 => .{ .Float32 = value },
                64 => .{ .Float64 = value },
                else => error.UnsupportedType,
            },
            .Array => |info| if (info.child == u8) .{
                .String = value,
            } else blk: {
                var array_values = try self.allocator.alloc(Value, value.len);
                for (value, 0..) |item, i| {
                    array_values[i] = try self.convertToValue(item);
                }
                break :blk .{ .Array = array_values };
            },
            else => error.UnsupportedType,
        };
    }

    pub fn build(self: *QueryBuilder) ![]const u8 {
        return try self.buffer.toOwnedSlice();
    }
};