const std = @import("std");

pub const ClickHouseType = enum {
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    String,
    FixedString,
    DateTime,
    Date,
    UUID,
    Array,
    Nullable,
    Decimal,
    Decimal32,
    Decimal64,
    Decimal128,
    Enum8,
    Enum16,
    Map,
    Tuple,
    
    pub fn fromStr(type_str: []const u8) !ClickHouseType {
        if (std.mem.eql(u8, type_str, "Int8")) return .Int8;
        if (std.mem.eql(u8, type_str, "Int16")) return .Int16;
        if (std.mem.eql(u8, type_str, "Int32")) return .Int32;
        if (std.mem.eql(u8, type_str, "Int64")) return .Int64;
        if (std.mem.eql(u8, type_str, "UInt8")) return .UInt8;
        if (std.mem.eql(u8, type_str, "UInt16")) return .UInt16;
        if (std.mem.eql(u8, type_str, "UInt32")) return .UInt32;
        if (std.mem.eql(u8, type_str, "UInt64")) return .UInt64;
        if (std.mem.eql(u8, type_str, "Float32")) return .Float32;
        if (std.mem.eql(u8, type_str, "Float64")) return .Float64;
        if (std.mem.eql(u8, type_str, "String")) return .String;
        if (std.mem.startsWith(u8, type_str, "FixedString")) return .FixedString;
        if (std.mem.eql(u8, type_str, "DateTime")) return .DateTime;
        if (std.mem.eql(u8, type_str, "Date")) return .Date;
        if (std.mem.eql(u8, type_str, "UUID")) return .UUID;
        if (std.mem.startsWith(u8, type_str, "Array")) return .Array;
        if (std.mem.startsWith(u8, type_str, "Nullable")) return .Nullable;
        if (std.mem.startsWith(u8, type_str, "Decimal")) {
            if (std.mem.startsWith(u8, type_str, "Decimal32")) return .Decimal32;
            if (std.mem.startsWith(u8, type_str, "Decimal64")) return .Decimal64;
            if (std.mem.startsWith(u8, type_str, "Decimal128")) return .Decimal128;
            return .Decimal;
        }
        if (std.mem.startsWith(u8, type_str, "Enum8")) return .Enum8;
        if (std.mem.startsWith(u8, type_str, "Enum16")) return .Enum16;
        if (std.mem.startsWith(u8, type_str, "Map")) return .Map;
        if (std.mem.startsWith(u8, type_str, "Tuple")) return .Tuple;
        return error.UnsupportedType;
    }

    pub fn isDecimal(self: ClickHouseType) bool {
        return switch (self) {
            .Decimal, .Decimal32, .Decimal64, .Decimal128 => true,
            else => false,
        };
    }

    pub fn isEnum(self: ClickHouseType) bool {
        return switch (self) {
            .Enum8, .Enum16 => true,
            else => false,
        };
    }
};

pub const TypeInfo = struct {
    base_type: ClickHouseType,
    nullable: bool = false,
    precision: ?u8 = null,
    scale: ?u8 = null,
    array_level: u8 = 0,
    fixed_string_length: ?u32 = null,
    enum_values: ?[]const EnumValue = null,
    tuple_types: ?[]const TypeInfo = null,
    map_key_type: ?*const TypeInfo = null,
    map_value_type: ?*const TypeInfo = null,

    pub const EnumValue = struct {
        name: []const u8,
        value: i16,
    };

    pub fn parse(allocator: std.mem.Allocator, type_str: []const u8) !TypeInfo {
        var result = TypeInfo{
            .base_type = undefined,
        };

        if (std.mem.startsWith(u8, type_str, "Nullable(")) {
            result.nullable = true;
            const inner_type = type_str[9 .. type_str.len - 1];
            const inner_info = try parse(allocator, inner_type);
            result.base_type = inner_info.base_type;
            result.precision = inner_info.precision;
            result.scale = inner_info.scale;
            result.array_level = inner_info.array_level;
            result.fixed_string_length = inner_info.fixed_string_length;
            result.enum_values = inner_info.enum_values;
            result.tuple_types = inner_info.tuple_types;
            return result;
        }

        if (std.mem.startsWith(u8, type_str, "Array(")) {
            var inner_type = type_str[6 .. type_str.len - 1];
            var inner_info = try parse(allocator, inner_type);
            inner_info.array_level += 1;
            return inner_info;
        }

        // Handle other complex types...
        if (std.mem.startsWith(u8, type_str, "FixedString(")) {
            result.base_type = .FixedString;
            const len_str = type_str[11 .. type_str.len - 1];
            result.fixed_string_length = try std.fmt.parseInt(u32, len_str, 10);
            return result;
        }

        result.base_type = try ClickHouseType.fromStr(type_str);
        return result;
    }
};