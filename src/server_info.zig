const std = @import("std");

pub const ServerInfo = struct {
    name: []const u8,
    major_version: u64,
    minor_version: u64,
    revision: u64,
    timezone: []const u8,
    display_name: []const u8,
    version_patch: u64,

    pub fn read(allocator: std.mem.Allocator, reader: anytype) !ServerInfo {
        var name_len = try reader.readIntLittle(u8);
        var name_buf = try allocator.alloc(u8, name_len);
        _ = try reader.readAll(name_buf);

        const major_version = try reader.readIntLittle(u64);
        const minor_version = try reader.readIntLittle(u64);
        const revision = try reader.readIntLittle(u64);

        var tz_len = try reader.readIntLittle(u8);
        var tz_buf = try allocator.alloc(u8, tz_len);
        _ = try reader.readAll(tz_buf);

        var display_len = try reader.readIntLittle(u8);
        var display_buf = try allocator.alloc(u8, display_len);
        _ = try reader.readAll(display_buf);

        const version_patch = try reader.readIntLittle(u64);

        return ServerInfo{
            .name = name_buf,
            .major_version = major_version,
            .minor_version = minor_version,
            .revision = revision,
            .timezone = tz_buf,
            .display_name = display_buf,
            .version_patch = version_patch,
        };
    }

    pub fn deinit(self: *ServerInfo, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        allocator.free(self.timezone);
        allocator.free(self.display_name);
    }
};