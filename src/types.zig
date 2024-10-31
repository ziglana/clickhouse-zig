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
    LowCardinality,
    Nested,
    
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
        if (std.mem.startsWith(u8, type_str, "LowCardinality")) return .LowCardinality;
        if (std.mem.startsWith(u8, type_str, "Nested")) return .Nested;
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

    pub fn isComplex(self: ClickHouseType) bool {
        return switch (self) {
            .Array, .Nullable, .Map, .Tuple, .LowCardinality, .Nested => true,
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
    low_cardinality_type: ?*const TypeInfo = null,
    nested_types: ?[]const NestedField = null,

    pub const EnumValue = struct {
        name: []const u8,
        value: i16,
    };

    pub const NestedField = struct {
        name: []const u8,
        type_info: TypeInfo,
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

        if (std.mem.startsWith(u8, type_str, "LowCardinality(")) {
            const inner_type = type_str[14 .. type_str.len - 1];
            var inner_info = try parse(allocator, inner_type);
            result.base_type = .LowCardinality;
            result.low_cardinality_type = try allocator.create(TypeInfo);
            result.low_cardinality_type.?.* = inner_info;
            return result;
        }

        if (std.mem.startsWith(u8, type_str, "Map(")) {
            result.base_type = .Map;
            const map_types = type_str[4 .. type_str.len - 1];
            var it = std.mem.split(u8, map_types, ",");
            const key_type_str = it.next() orelse return error.InvalidMapType;
            const value_type_str = it.next() orelse return error.InvalidMapType;
            
            result.map_key_type = try allocator.create(TypeInfo);
            result.map_value_type = try allocator.create(TypeInfo);
            result.map_key_type.?.* = try parse(allocator, std.mem.trim(u8, key_type_str, " "));
            result.map_value_type.?.* = try parse(allocator, std.mem.trim(u8, value_type_str, " "));
            return result;
        }

        if (std.mem.startsWith(u8, type_str, "Nested(")) {
            result.base_type = .Nested;
            const nested_fields = type_str[7 .. type_str.len - 1];
            var fields = std.ArrayList(NestedField).init(allocator);
            defer fields.deinit();

            var it = std.mem.split(u8, nested_fields, ",");
            while (it.next()) |field| {
                const trimmed = std.mem.trim(u8, field, " ");
                var field_parts = std.mem.split(u8, trimmed, " ");
                const field_name = field_parts.next() orelse return error.InvalidNestedType;
                const field_type = field_parts.next() orelse return error.InvalidNestedType;
                
                try fields.append(.{
                    .name = try allocator.dupe(u8, field_name),
                    .type_info = try parse(allocator, field_type),
                });
            }

            result.nested_types = try fields.toOwnedSlice();
            return result;
        }

        result.base_type = try ClickHouseType.fromStr(type_str);
        return result;
    }

    pub fn deinit(self: *TypeInfo, allocator: std.mem.Allocator) void {
        if (self.map_key_type) |key_type| {
            key_type.deinit(allocator);
            allocator.destroy(key_type);
        }
        if (self.map_value_type) |value_type| {
            value_type.deinit(allocator);
            allocator.destroy(value_type);
        }
        if (self.low_cardinality_type) |lc_type| {
            lc_type.deinit(allocator);
            allocator.destroy(lc_type);
        }
        if (self.nested_types) |nested| {
            for (nested) |*field| {
                allocator.free(field.name);
                field.type_info.deinit(allocator);
            }
            allocator.free(nested);
        }
    }
};