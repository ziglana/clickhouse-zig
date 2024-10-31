const std = @import("std");

pub const ErrorCode = enum(u32) {
    Unknown = 0,
    NetworkError = 1,
    BadResponse = 2,
    ParseError = 3,
    TypeMismatch = 4,
    Timeout = 5,
    Cancelled = 6,
    ServerError = 7,
    ProtocolError = 8,
    CompressionError = 9,
    
    pub fn fromInt(code: u32) ErrorCode {
        return std.meta.intToEnum(ErrorCode, code) catch .Unknown;
    }
};

pub const Error = struct {
    code: ErrorCode,
    message: []const u8,
    stack_trace: ?[]const u8,
    nested: ?*Error,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, code: ErrorCode, message: []const u8) !*Error {
        var err = try allocator.create(Error);
        err.* = .{
            .code = code,
            .message = try allocator.dupe(u8, message),
            .stack_trace = null,
            .nested = null,
            .allocator = allocator,
        };
        return err;
    }

    pub fn initWithStack(allocator: std.mem.Allocator, code: ErrorCode, message: []const u8, stack_trace: []const u8) !*Error {
        var err = try allocator.create(Error);
        err.* = .{
            .code = code,
            .message = try allocator.dupe(u8, message),
            .stack_trace = try allocator.dupe(u8, stack_trace),
            .nested = null,
            .allocator = allocator,
        };
        return err;
    }

    pub fn wrap(allocator: std.mem.Allocator, code: ErrorCode, message: []const u8, nested: *Error) !*Error {
        var err = try allocator.create(Error);
        err.* = .{
            .code = code,
            .message = try allocator.dupe(u8, message),
            .stack_trace = null,
            .nested = nested,
            .allocator = allocator,
        };
        return err;
    }

    pub fn deinit(self: *Error) void {
        self.allocator.free(self.message);
        if (self.stack_trace) |st| {
            self.allocator.free(st);
        }
        if (self.nested) |nested| {
            nested.deinit();
            self.allocator.destroy(nested);
        }
        self.allocator.destroy(self);
    }

    pub fn format(self: Error, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
        _ = fmt;
        _ = options;

        try writer.print("{s}: {s}", .{ @tagName(self.code), self.message });
        if (self.stack_trace) |st| {
            try writer.print("\nStack trace:\n{s}", .{st});
        }
        if (self.nested) |nested| {
            try writer.print("\nCaused by: ", .{});
            try nested.format(fmt, options, writer);
        }
    }
};