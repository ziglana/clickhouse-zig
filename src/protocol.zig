const std = @import("std");
const compression = @import("compression.zig");

pub const ClientHello = struct {
    const CLIENT_NAME = "ClickHouse Zig Client";
    const CLIENT_VERSION_MAJOR: u64 = 1;
    const CLIENT_VERSION_MINOR: u64 = 0;
    const PROTOCOL_VERSION: u64 = 54429;

    pub fn write(writer: anytype) !void {
        try writer.writeAll("ClickHouseClient");
        try writer.writeIntLittle(u64, CLIENT_VERSION_MAJOR);
        try writer.writeIntLittle(u64, CLIENT_VERSION_MINOR);
        try writer.writeIntLittle(u64, PROTOCOL_VERSION);
    }
};

pub const ClientInfo = struct {
    pub fn write(writer: anytype, query_id: []const u8, client_name: []const u8) !void {
        // Query ID
        try writer.writeIntLittle(u8, @as(u8, @truncate(query_id.len)));
        try writer.writeAll(query_id);

        // Client info block
        try writer.writeIntLittle(u8, 1); // client_info marker
        try writer.writeIntLittle(u8, 1); // major protocol version
        try writer.writeIntLittle(u8, @as(u8, @truncate(client_name.len)));
        try writer.writeAll(client_name);
        try writer.writeIntLittle(u8, 0); // query kind
        
        // Interface (TCP)
        try writer.writeIntLittle(u8, 1);
        
        // Initial address
        try writer.writeIntLittle(u8, 0);
        
        // Initial port
        try writer.writeIntLittle(u16, 0);
        
        // Compression method
        try writer.writeIntLittle(u8, @intFromEnum(compression.CompressionMethod.None));
    }
};