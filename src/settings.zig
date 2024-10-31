const std = @import("std");

pub const Settings = struct {
    max_block_size: u64 = 65536,
    connect_timeout_ms: u64 = 10000,
    receive_timeout_ms: u64 = 10000,
    send_timeout_ms: u64 = 10000,
    compression_method: u8 = 0, // None by default
    decompress_response: bool = true,
    
    pub fn write(self: Settings, writer: anytype) !void {
        // Write number of settings
        try writer.writeIntLittle(u64, 5);
        
        // Write each setting
        try writeSetting(writer, "max_block_size", self.max_block_size);
        try writeSetting(writer, "connect_timeout_ms", self.connect_timeout_ms);
        try writeSetting(writer, "receive_timeout_ms", self.receive_timeout_ms);
        try writeSetting(writer, "send_timeout_ms", self.send_timeout_ms);
        try writeSetting(writer, "compression_method", self.compression_method);
    }

    fn writeSetting(writer: anytype, name: []const u8, value: anytype) !void {
        try writer.writeIntLittle(u8, @as(u8, @truncate(name.len)));
        try writer.writeAll(name);
        
        const T = @TypeOf(value);
        switch (T) {
            u64 => {
                try writer.writeIntLittle(u8, 1); // UInt64 type
                try writer.writeIntLittle(u64, value);
            },
            u8 => {
                try writer.writeIntLittle(u8, 0); // UInt8 type
                try writer.writeIntLittle(u8, value);
            },
            else => @compileError("Unsupported setting type"),
        }
    }
};