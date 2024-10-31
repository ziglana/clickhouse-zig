const std = @import("std");
const types = @import("types.zig");
const complex_types = @import("complex_types.zig");

pub const SerializationError = error{
    InvalidType,
    BufferTooSmall,
    InvalidData,
    UnsupportedType,
};

pub fn serializeLowCardinality(writer: anytype, value: complex_types.LowCardinality) !void {
    // Write dictionary size
    try writer.writeIntLittle(u64, value.dictionary_size);

    // Write dictionary entries
    switch (value.base_type.base_type) {
        .String => {
            // String dictionary format:
            // - key (index)
            // - length of string
            // - string data
            for (value.dictionary) |entry| {
                try writer.writeIntLittle(u32, entry.key);
                try writer.writeIntLittle(u32, @intCast(u32, entry.value.len));
                try writer.writeAll(entry.value);
            }
        },
        .Int8, .Int16, .Int32, .Int64,
        .UInt8, .UInt16, .UInt32, .UInt64,
        .Float32, .Float64 => {
            // Numeric dictionary format:
            // - key (index)
            // - value
            for (value.dictionary) |entry| {
                try writer.writeIntLittle(u32, entry.key);
                try serializeNumeric(writer, entry.value, value.base_type.base_type);
            }
        },
        else => return SerializationError.UnsupportedType,
    }
}

pub fn deserializeLowCardinality(reader: anytype, allocator: std.mem.Allocator, type_info: types.TypeInfo) !complex_types.LowCardinality {
    const dict_size = try reader.readIntLittle(u64);
    var result = complex_types.LowCardinality.init(type_info.low_cardinality_type.?.*);
    result.dictionary_size = dict_size;

    var dictionary = std.ArrayList(complex_types.LowCardinality.DictionaryEntry).init(allocator);
    errdefer dictionary.deinit();

    var i: usize = 0;
    while (i < dict_size) : (i += 1) {
        const key = try reader.readIntLittle(u32);
        const value = switch (type_info.low_cardinality_type.?.base_type) {
            .String => {
                const len = try reader.readIntLittle(u32);
                const str = try allocator.alloc(u8, len);
                errdefer allocator.free(str);
                try reader.readNoEof(str);
                .{ .String = str };
            },
            else => try deserializeNumeric(reader, type_info.low_cardinality_type.?.base_type),
        };

        try dictionary.append(.{ .key = key, .value = value });
    }

    result.dictionary = try dictionary.toOwnedSlice();
    return result;
}

pub fn serializeNested(writer: anytype, value: complex_types.Nested) !void {
    // Write number of fields
    try writer.writeIntLittle(u32, @intCast(u32, value.types.len));

    // Write each field's data
    for (value.types) |field_type| {
        try writer.writeIntLittle(u32, @intCast(u32, field_type.name.len));
        try writer.writeAll(field_type.name);
        try serializeTypeInfo(writer, field_type);
    }
}

pub fn deserializeNested(reader: anytype, allocator: std.mem.Allocator) !complex_types.Nested {
    const field_count = try reader.readIntLittle(u32);
    var field_types = try allocator.alloc(types.TypeInfo, field_count);
    errdefer allocator.free(field_types);

    var i: usize = 0;
    while (i < field_count) : (i += 1) {
        const name_len = try reader.readIntLittle(u32);
        const name = try allocator.alloc(u8, name_len);
        errdefer allocator.free(name);
        try reader.readNoEof(name);

        field_types[i] = try deserializeTypeInfo(reader, allocator);
        field_types[i].name = name;
    }

    return complex_types.Nested.init(allocator, "nested", field_types);
}

pub fn serializeMap(writer: anytype, value: complex_types.Map) !void {
    // Write key type info
    try serializeTypeInfo(writer, value.key_type);

    // Write value type info
    try serializeTypeInfo(writer, value.value_type);

    // Write if nullable
    try writer.writeByte(if (value.nullable) 1 else 0);
}

pub fn deserializeMap(reader: anytype, allocator: std.mem.Allocator) !complex_types.Map {
    const key_type = try deserializeTypeInfo(reader, allocator);
    const value_type = try deserializeTypeInfo(reader, allocator);
    const nullable = (try reader.readByte()) != 0;

    var result = complex_types.Map.init(key_type, value_type);
    result.nullable = nullable;
    return result;
}

fn serializeTypeInfo(writer: anytype, type_info: types.TypeInfo) !void {
    try writer.writeIntLittle(u8, @enumToInt(type_info.base_type));
    try writer.writeByte(if (type_info.nullable) 1 else 0);

    if (type_info.precision) |p| {
        try writer.writeByte(1);
        try writer.writeIntLittle(u8, p);
    } else {
        try writer.writeByte(0);
    }

    if (type_info.scale) |s| {
        try writer.writeByte(1);
        try writer.writeIntLittle(u8, s);
    } else {
        try writer.writeByte(0);
    }

    try writer.writeIntLittle(u8, type_info.array_level);
}

fn deserializeTypeInfo(reader: anytype, allocator: std.mem.Allocator) !types.TypeInfo {
    const base_type = @intToEnum(types.ClickHouseType, try reader.readIntLittle(u8));
    const nullable = (try reader.readByte()) != 0;

    const has_precision = (try reader.readByte()) != 0;
    const precision = if (has_precision) try reader.readIntLittle(u8) else null;

    const has_scale = (try reader.readByte()) != 0;
    const scale = if (has_scale) try reader.readIntLittle(u8) else null;

    const array_level = try reader.readIntLittle(u8);

    return types.TypeInfo{
        .base_type = base_type,
        .nullable = nullable,
        .precision = precision,
        .scale = scale,
        .array_level = array_level,
        .fixed_string_length = null,
        .enum_values = null,
        .tuple_types = null,
        .map_key_type = null,
        .map_value_type = null,
        .low_cardinality_type = null,
        .nested_types = null,
    };
}

fn serializeNumeric(writer: anytype, value: anytype, base_type: types.ClickHouseType) !void {
    switch (base_type) {
        .Int8 => try writer.writeIntLittle(i8, value),
        .Int16 => try writer.writeIntLittle(i16, value),
        .Int32 => try writer.writeIntLittle(i32, value),
        .Int64 => try writer.writeIntLittle(i64, value),
        .UInt8 => try writer.writeIntLittle(u8, value),
        .UInt16 => try writer.writeIntLittle(u16, value),
        .UInt32 => try writer.writeIntLittle(u32, value),
        .UInt64 => try writer.writeIntLittle(u64, value),
        .Float32 => try writer.writeIntLittle(f32, value),
        .Float64 => try writer.writeIntLittle(f64, value),
        else => return SerializationError.UnsupportedType,
    }
}

fn deserializeNumeric(reader: anytype, base_type: types.ClickHouseType) !Value {
    return switch (base_type) {
        .Int8 => .{ .Int8 = try reader.readIntLittle(i8) },
        .Int16 => .{ .Int16 = try reader.readIntLittle(i16) },
        .Int32 => .{ .Int32 = try reader.readIntLittle(i32) },
        .Int64 => .{ .Int64 = try reader.readIntLittle(i64) },
        .UInt8 => .{ .UInt8 = try reader.readIntLittle(u8) },
        .UInt16 => .{ .UInt16 = try reader.readIntLittle(u16) },
        .UInt32 => .{ .UInt32 = try reader.readIntLittle(u32) },
        .UInt64 => .{ .UInt64 = try reader.readIntLittle(u64) },
        .Float32 => .{ .Float32 = try reader.readIntLittle(f32) },
        .Float64 => .{ .Float64 = try reader.readIntLittle(f64) },
        else => SerializationError.UnsupportedType,
    };
}

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
};