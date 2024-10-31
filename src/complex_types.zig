const std = @import("std");
const types = @import("types.zig");

pub const LowCardinality = struct {
    base_type: types.TypeInfo,
    dictionary: []DictionaryEntry,
    dictionary_size: usize,

    pub const DictionaryEntry = struct {
        key: u32,
        value: Value,
    };

    pub const Value = union(enum) {
        String: []const u8,
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
    };

    pub fn init(base_type: types.TypeInfo) LowCardinality {
        return .{
            .base_type = base_type,
            .dictionary = &[_]DictionaryEntry{},
            .dictionary_size = 0,
        };
    }

    pub fn deinit(self: *LowCardinality, allocator: std.mem.Allocator) void {
        for (self.dictionary) |entry| {
            switch (entry.value) {
                .String => |s| allocator.free(s),
                else => {},
            }
        }
        allocator.free(self.dictionary);
    }
};

pub const AggregateFunction = struct {
    name: []const u8,
    argument_types: []types.TypeInfo,
    return_type: types.TypeInfo,
    
    pub fn init(
        allocator: std.mem.Allocator,
        name: []const u8,
        argument_types: []const types.TypeInfo,
        return_type: types.TypeInfo,
    ) !AggregateFunction {
        return AggregateFunction{
            .name = try allocator.dupe(u8, name),
            .argument_types = try allocator.dupe(types.TypeInfo, argument_types),
            .return_type = return_type,
        };
    }

    pub fn deinit(self: *AggregateFunction, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        allocator.free(self.argument_types);
    }
};

pub const SimpleAggregateFunction = struct {
    name: []const u8,
    argument_type: types.TypeInfo,
    
    pub fn init(allocator: std.mem.Allocator, name: []const u8, argument_type: types.TypeInfo) !SimpleAggregateFunction {
        return SimpleAggregateFunction{
            .name = try allocator.dupe(u8, name),
            .argument_type = argument_type,
        };
    }

    pub fn deinit(self: *SimpleAggregateFunction, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
    }
};

pub const Nested = struct {
    name: []const u8,
    types: []types.TypeInfo,
    nullable: bool,

    pub fn init(allocator: std.mem.Allocator, name: []const u8, field_types: []const types.TypeInfo) !Nested {
        var types_copy = try allocator.alloc(types.TypeInfo, field_types.len);
        @memcpy(types_copy, field_types);

        return Nested{
            .name = try allocator.dupe(u8, name),
            .types = types_copy,
            .nullable = false,
        };
    }

    pub fn deinit(self: *Nested, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        allocator.free(self.types);
    }
};

pub const Map = struct {
    key_type: types.TypeInfo,
    value_type: types.TypeInfo,
    nullable: bool,

    pub fn init(key_type: types.TypeInfo, value_type: types.TypeInfo) Map {
        return .{
            .key_type = key_type,
            .value_type = value_type,
            .nullable = false,
        };
    }
};

pub const Point = struct {
    x: f64,
    y: f64,
};

pub const Ring = struct {
    points: []Point,
    
    pub fn init(allocator: std.mem.Allocator, points: []const Point) !Ring {
        return Ring{
            .points = try allocator.dupe(Point, points),
        };
    }

    pub fn deinit(self: *Ring, allocator: std.mem.Allocator) void {
        allocator.free(self.points);
    }
};

pub const Polygon = struct {
    rings: []Ring,
    
    pub fn init(allocator: std.mem.Allocator, rings: []const Ring) !Polygon {
        return Polygon{
            .rings = try allocator.dupe(Ring, rings),
        };
    }

    pub fn deinit(self: *Polygon, allocator: std.mem.Allocator) void {
        for (self.rings) |*ring| {
            ring.deinit(allocator);
        }
        allocator.free(self.rings);
    }
};

pub const MultiPolygon = struct {
    polygons: []Polygon,
    
    pub fn init(allocator: std.mem.Allocator, polygons: []const Polygon) !MultiPolygon {
        return MultiPolygon{
            .polygons = try allocator.dupe(Polygon, polygons),
        };
    }

    pub fn deinit(self: *MultiPolygon, allocator: std.mem.Allocator) void {
        for (self.polygons) |*polygon| {
            polygon.deinit(allocator);
        }
        allocator.free(self.polygons);
    }
};

pub const IPv4 = struct {
    octets: [4]u8,
};

pub const IPv6 = struct {
    segments: [8]u16,
};

pub const JSON = struct {
    value: std.json.Value,
    
    pub fn init(allocator: std.mem.Allocator, json_str: []const u8) !JSON {
        const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_str, .{});
        return JSON{
            .value = parsed.value,
        };
    }

    pub fn deinit(self: *JSON, allocator: std.mem.Allocator) void {
        self.value.deinit(allocator);
    }
};