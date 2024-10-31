const std = @import("std");
const params = @import("params.zig");
const types = @import("types.zig");

pub const PreparedStatement = struct {
    query: []const u8,
    parameters: std.ArrayList(params.Parameter),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, query: []const u8) PreparedStatement {
        return .{
            .query = allocator.dupe(u8, query) catch unreachable,
            .parameters = std.ArrayList(params.Parameter).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *PreparedStatement) void {
        self.allocator.free(self.query);
        for (self.parameters.items) |*param| {
            param.deinit();
        }
        self.parameters.deinit();
    }

    pub fn bind(self: *PreparedStatement, name: []const u8, value: anytype) !void {
        const param = try params.Parameter.init(self.allocator, name, value);
        try self.parameters.append(param);
    }

    pub fn clearParameters(self: *PreparedStatement) void {
        for (self.parameters.items) |*param| {
            param.deinit();
        }
        self.parameters.clearRetainingCapacity();
    }

    pub fn buildQuery(self: *PreparedStatement) ![]const u8 {
        var result = std.ArrayList(u8).init(self.allocator);
        defer result.deinit();

        var query_iter = std.mem.split(u8, self.query, "?");
        var param_index: usize = 0;

        while (query_iter.next()) |part| {
            try result.appendSlice(part);
            
            if (param_index < self.parameters.items.len) {
                const param = self.parameters.items[param_index];
                try self.appendParameterValue(&result, param);
                param_index += 1;
            }
        }

        return result.toOwnedSlice();
    }

    fn appendParameterValue(self: *PreparedStatement, result: *std.ArrayList(u8), param: params.Parameter) !void {
        switch (param.value) {
            .Int8 => |v| try result.writer().print("{}", .{v}),
            .Int16 => |v| try result.writer().print("{}", .{v}),
            .Int32 => |v| try result.writer().print("{}", .{v}),
            .Int64 => |v| try result.writer().print("{}", .{v}),
            .UInt8 => |v| try result.writer().print("{}", .{v}),
            .UInt16 => |v| try result.writer().print("{}", .{v}),
            .UInt32 => |v| try result.writer().print("{}", .{v}),
            .UInt64 => |v| try result.writer().print("{}", .{v}),
            .Float32 => |v| try result.writer().print("{d}", .{v}),
            .Float64 => |v| try result.writer().print("{d}", .{v}),
            .String => |v| try result.writer().print("'{s}'", .{v}),
            .DateTime => |v| try result.writer().print("{}", .{v}),
            .Date => |v| try result.writer().print("{}", .{v}),
            .UUID => |v| try result.writer().print("{}", .{std.fmt.fmtSliceHexLower(&v)}),
            .Array => |arr| {
                try result.append('[');
                for (arr, 0..) |item, i| {
                    if (i > 0) try result.appendSlice(", ");
                    try self.appendParameterValue(result, .{
                        .name = "",
                        .type_info = param.type_info,
                        .value = item,
                        .allocator = self.allocator,
                    });
                }
                try result.append(']');
            },
            .Null => try result.appendSlice("NULL"),
        }
    }
};