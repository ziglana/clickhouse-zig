const std = @import("std");
const types = @import("types.zig");

pub const Parameter = struct {
    name: []const u8,
    type_info: types.TypeInfo,
    value: Value,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, name: []const u8, value: anytype) !Parameter {
        const T = @TypeOf(value);
        const type_info = try inferTypeInfo(T);
        const param_value = try convertToValue(allocator, value);

        return Parameter{
            .name = try allocator.dupe(u8, name),
            .type_info = type_info,
            .value = param_value,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Parameter) void {
        self.allocator.free(self.name);
        switch (self.value) {
            .String => |s| self.allocator.free(s),
            .Array => |arr| {
                for (arr) |item| {
                    switch (item) {
                        .String => |s| self.allocator.free(s),
                        else => {},
                    }
                }
                self.allocator.free(arr);
            },
            else => {},
        }
    }
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
    Array: []Value,
    Null,
};

fn inferTypeInfo(comptime T: type) !types.TypeInfo {
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

fn convertToValue(allocator: std.mem.Allocator, value: anytype) !Value {
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
            .String = try allocator.dupe(u8, value),
        } else blk: {
            var array_values = try allocator.alloc(Value, value.len);
            for (value, 0..) |item, i| {
                array_values[i] = try convertToValue(allocator, item);
            }
            break :blk .{ .Array = array_values };
        },
        else => error.UnsupportedType,
    };
}