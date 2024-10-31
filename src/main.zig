const std = @import("std");
const net = std.net;
const mem = std.mem;
const protocol = @import("protocol.zig");
const packet = @import("packet.zig");
const block = @import("block.zig");
const settings = @import("settings.zig");
const compression = @import("compression.zig");
const results = @import("results.zig");
const error = @import("error.zig");
const server_info = @import("server_info.zig");
const query_info = @import("query_info.zig");
const progress = @import("progress.zig");
const statistics = @import("statistics.zig");
const profile = @import("profile_info.zig");

pub const ClickHouseError = error{
    ConnectionFailed,
    QueryFailed,
    InvalidResponse,
    OutOfMemory,
    ProtocolError,
    CompressionError,
    TypeMismatch,
    QueryCancelled,
};

pub const ClickHouseConfig = struct {
    host: []const u8,
    port: u16 = 9000,
    username: []const u8 = "default",
    password: []const u8 = "",
    database: []const u8 = "default",
    settings: settings.Settings = .{},
};

pub const ClickHouseClient = struct {
    config: ClickHouseConfig,
    stream: ?net.Stream,
    allocator: mem.Allocator,
    current_block: ?block.Block,
    current_result: ?results.QueryResult,
    server_info: ?server_info.ServerInfo,
    query_info: ?query_info.QueryInfo,
    last_error: ?*error.Error,

    pub fn init(allocator: mem.Allocator, config: ClickHouseConfig) ClickHouseClient {
        return .{
            .config = config,
            .stream = null,
            .allocator = allocator,
            .current_block = null,
            .current_result = null,
            .server_info = null,
            .query_info = null,
            .last_error = null,
        };
    }

    pub fn connect(self: *ClickHouseClient) !void {
        const address = try net.Address.parseIp(self.config.host, self.config.port);
        self.stream = try net.tcpConnectToAddress(address);

        try self.sendHello();
        try self.readServerHello();
    }

    fn sendHello(self: *ClickHouseClient) !void {
        var writer = self.stream.?.writer();
        
        try packet.writePacketHeader(writer, .Hello);
        try protocol.ClientHello.write(writer);
        
        try writer.writeIntLittle(u8, @as(u8, @truncate(self.config.database.len)));
        try writer.writeAll(self.config.database);
        
        try writer.writeIntLittle(u8, @as(u8, @truncate(self.config.username.len)));
        try writer.writeAll(self.config.username);
        
        try writer.writeIntLittle(u8, @as(u8, @truncate(self.config.password.len)));
        try writer.writeAll(self.config.password);
    }

    fn readServerHello(self: *ClickHouseClient) !void {
        var reader = self.stream.?.reader();
        
        const server_packet = try reader.readIntLittle(u64);
        if (server_packet != @intFromEnum(packet.PacketType.Hello)) {
            return ClickHouseError.ProtocolError;
        }

        self.server_info = try server_info.ServerInfo.read(self.allocator, reader);
    }

    pub fn query(self: *ClickHouseClient, query_str: []const u8) !void {
        if (self.stream == null) {
            return ClickHouseError.ConnectionFailed;
        }

        // Clean up previous results if any
        if (self.current_result) |*result| {
            result.deinit();
            self.current_result = null;
        }

        if (self.query_info) |*info| {
            _ = info;
            self.query_info = null;
        }

        self.query_info = query_info.QueryInfo.init();

        var writer = self.stream.?.writer();

        try packet.writePacketHeader(writer, .Query);
        try protocol.ClientInfo.write(writer, "", "ClickHouse Zig");
        try self.config.settings.write(writer);

        try writer.writeIntLittle(u64, query_str.len);
        try writer.writeAll(query_str);

        try self.processQueryResponse();
    }

    fn processQueryResponse(self: *ClickHouseClient) !void {
        var reader = self.stream.?.reader();
        
        while (true) {
            const packet_type = try reader.readIntLittle(u64);
            
            switch (@as(packet.PacketType, @enumFromInt(packet_type))) {
                .Data => {
                    if (self.current_block == null) {
                        self.current_block = block.Block.init(self.allocator);
                    }
                    try self.readBlock();
                    
                    if (self.current_block) |*b| {
                        self.current_result = try results.QueryResult.init(self.allocator, b);
                    }
                },
                .Progress => {
                    const prog = try progress.Progress.read(reader);
                    if (self.query_info) |*info| {
                        info.updateProgress(prog);
                    }
                },
                .ProfileInfo => {
                    const prof = try profile.ProfileInfo.read(reader);
                    if (self.query_info) |*info| {
                        info.updateProfile(prof);
                    }
                },
                .Statistics => {
                    const stats = try statistics.Statistics.read(reader);
                    if (self.query_info) |*info| {
                        info.updateStatistics(stats);
                    }
                },
                .EndOfStream => return,
                .Error => {
                    const err_code = try reader.readIntLittle(u32);
                    const msg_len = try reader.readIntLittle(u16);
                    var msg_buf = try self.allocator.alloc(u8, msg_len);
                    defer self.allocator.free(msg_buf);
                    _ = try reader.readAll(msg_buf);

                    const stack_len = try reader.readIntLittle(u16);
                    var stack_buf: ?[]u8 = null;
                    if (stack_len > 0) {
                        stack_buf = try self.allocator.alloc(u8, stack_len);
                        _ = try reader.readAll(stack_buf.?);
                    }

                    self.last_error = try error.Error.initWithStack(
                        self.allocator,
                        error.ErrorCode.fromInt(err_code),
                        msg_buf,
                        if (stack_buf) |sb| sb else "",
                    );

                    return ClickHouseError.QueryFailed;
                },
                else => {},
            }
        }
    }

    pub fn deinit(self: *ClickHouseClient) void {
        if (self.current_result) |*result| {
            result.deinit();
        }
        if (self.current_block) |*b| {
            b.deinit();
        }
        if (self.server_info) |*info| {
            info.deinit(self.allocator);
        }
        if (self.last_error) |err| {
            err.deinit();
        }
        if (self.stream) |stream| {
            stream.close();
        }
    }
};