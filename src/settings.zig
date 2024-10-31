const std = @import("std");

pub const Settings = struct {
    // Connection settings
    max_block_size: u64 = 65536,
    connect_timeout_ms: u64 = 10000,
    receive_timeout_ms: u64 = 10000,
    send_timeout_ms: u64 = 10000,
    tcp_keep_alive: bool = true,
    tcp_nodelay: bool = true,
    compression_method: u8 = 0,
    decompress_response: bool = true,
    
    // Query settings
    max_insert_block_size: u64 = 1048576,
    max_threads: u32 = 8,
    max_memory_usage: u64 = 0,
    prefer_localhost_replica: bool = true,
    totals_mode: TotalsMode = .AfterHavingGroupBy,
    quota_key: ?[]const u8 = null,
    priority: u32 = 0,
    load_balancing: LoadBalancing = .Random,
    max_execution_time: u64 = 0,
    max_rows_to_read: u64 = 0,
    max_bytes_to_read: u64 = 0,
    max_result_rows: u64 = 0,
    max_result_bytes: u64 = 0,
    result_overflow_mode: OverflowMode = .Break,
    
    pub const TotalsMode = enum {
        BeforeHavingGroupBy,
        AfterHavingGroupBy,
        OnlyFinal,
    };

    pub const LoadBalancing = enum {
        Random,
        NearestHost,
        InOrder,
        FirstOrRandom,
    };

    pub const OverflowMode = enum {
        Break,
        Throw,
        Any,
    };

    pub fn write(self: Settings, writer: anytype) !void {
        var settings_count: usize = 0;
        var settings_buf = std.ArrayList(u8).init(std.heap.page_allocator);
        defer settings_buf.deinit();

        // Write each setting
        inline for (std.meta.fields(Settings)) |field| {
            const value = @field(self, field.name);
            if (shouldWriteSetting(field.type, value)) {
                try writeSettingValue(&settings_buf, field.name, value);
                settings_count += 1;
            }
        }

        // Write settings count
        try writer.writeIntLittle(u64, settings_count);
        
        // Write settings buffer
        try writer.writeAll(settings_buf.items);
    }

    fn shouldWriteSetting(comptime T: type, value: T) bool {
        return switch (@typeInfo(T)) {
            .Optional => value != null,
            .Int, .Float => value != 0,
            .Bool => value != false,
            .Enum => true,
            else => false,
        };
    }

    fn writeSettingValue(buf: *std.ArrayList(u8), name: []const u8, value: anytype) !void {
        try buf.writer().writeIntLittle(u8, @intCast(u8, name.len));
        try buf.writer().writeAll(name);

        const T = @TypeOf(value);
        switch (@typeInfo(T)) {
            .Int => |info| {
                try buf.writer().writeIntLittle(u8, if (info.bits <= 8) 0 else 1);
                if (info.bits <= 8) {
                    try buf.writer().writeIntLittle(u8, @intCast(u8, value));
                } else {
                    try buf.writer().writeIntLittle(u64, value);
                }
            },
            .Float => {
                try buf.writer().writeIntLittle(u8, 3);
                try buf.writer().writeIntLittle(f64, value);
            },
            .Bool => {
                try buf.writer().writeIntLittle(u8, 0);
                try buf.writer().writeIntLittle(u8, @boolToInt(value));
            },
            .Enum => {
                try buf.writer().writeIntLittle(u8, 2);
                try buf.writer().writeAll(@tagName(value));
            },
            .Optional => if (value) |v| {
                try writeSettingValue(buf, name, v);
            },
            else => unreachable,
        }
    }
};