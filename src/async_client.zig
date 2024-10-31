const std = @import("std");
const net = std.net;
const ClickHouseConfig = @import("main.zig").ClickHouseConfig;
const protocol = @import("protocol.zig");
const packet = @import("packet.zig");
const block = @import("block.zig");
const error = @import("error.zig");
const tls = @import("tls.zig");

pub const AsyncClient = struct {
    config: ClickHouseConfig,
    stream: ?net.Stream,
    tls_ctx: ?*tls.TlsContext,
    allocator: std.mem.Allocator,
    frame: @Frame(queryAsync),

    pub fn init(allocator: std.mem.Allocator, config: ClickHouseConfig) AsyncClient {
        return .{
            .config = config,
            .stream = null,
            .tls_ctx = null,
            .allocator = allocator,
            .frame = undefined,
        };
    }

    pub fn deinit(self: *AsyncClient) void {
        if (self.tls_ctx) |ctx| {
            ctx.deinit();
        }
        if (self.stream) |stream| {
            stream.close();
        }
    }

    pub fn connect(self: *AsyncClient) !void {
        const address = try net.Address.parseIp(self.config.host, self.config.port);
        self.stream = try net.tcpConnectToAddress(address);

        if (self.config.tls_config) |tls_config| {
            self.tls_ctx = try tls.TlsContext.init(self.allocator, tls_config);
            try self.tls_ctx.?.connect(self.stream.?);
        }

        try self.sendHello();
        try self.readServerHello();
    }

    pub fn queryAsync(self: *AsyncClient, query_str: []const u8) !void {
        if (self.stream == null) {
            return error.ConnectionFailed;
        }

        var writer = if (self.tls_ctx) |ctx| ctx.writer() else self.stream.?.writer();

        try packet.writePacketHeader(writer, .Query);
        try protocol.ClientInfo.write(writer, "", "ClickHouse Zig Async");
        try self.config.settings.write(writer);

        try writer.writeIntLittle(u64, query_str.len);
        try writer.writeAll(query_str);

        try self.processQueryResponseAsync();
    }

    fn processQueryResponseAsync(self: *AsyncClient) !void {
        var reader = if (self.tls_ctx) |ctx| ctx.reader() else self.stream.?.reader();
        
        while (true) {
            const packet_type = try reader.readIntLittle(u64);
            
            switch (@as(packet.PacketType, @enumFromInt(packet_type))) {
                .Data => {
                    suspend {
                        // Process data asynchronously
                        resume self.frame;
                    }
                },
                .Progress => {
                    suspend {
                        // Process progress asynchronously
                        resume self.frame;
                    }
                },
                .EndOfStream => return,
                .Error => {
                    const err = try error.Error.readFromServer(reader, self.allocator);
                    defer err.deinit();
                    return error.QueryFailed;
                },
                else => {},
            }
        }
    }

    fn sendHello(self: *AsyncClient) !void {
        var writer = if (self.tls_ctx) |ctx| ctx.writer() else self.stream.?.writer();
        
        try packet.writePacketHeader(writer, .Hello);
        try protocol.ClientHello.write(writer);
        
        try writer.writeIntLittle(u8, @as(u8, @truncate(self.config.database.len)));
        try writer.writeAll(self.config.database);
        
        try writer.writeIntLittle(u8, @as(u8, @truncate(self.config.username.len)));
        try writer.writeAll(self.config.username);
        
        try writer.writeIntLittle(u8, @as(u8, @truncate(self.config.password.len)));
        try writer.writeAll(self.config.password);
    }

    fn readServerHello(self: *AsyncClient) !void {
        var reader = if (self.tls_ctx) |ctx| ctx.reader() else self.stream.?.reader();
        
        const server_packet = try reader.readIntLittle(u64);
        if (server_packet != @intFromEnum(packet.PacketType.Hello)) {
            return error.ProtocolError;
        }

        _ = try protocol.ServerHello.read(reader);
    }
};