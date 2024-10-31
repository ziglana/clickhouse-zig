const std = @import("std");
const Allocator = std.mem.Allocator;
const lz4 = @import("compression/lz4.zig");
const zstd = @import("compression/zstd.zig");

pub const CompressionMethod = enum(u8) {
    None = 0,
    LZ4 = 1,
    ZSTD = 2,
};

pub const CompressedData = struct {
    method: CompressionMethod,
    uncompressed_size: usize,
    compressed_size: usize,
    data: []const u8,

    pub fn compress(allocator: Allocator, data: []const u8, method: CompressionMethod) !CompressedData {
        switch (method) {
            .None => {
                return CompressedData{
                    .method = .None,
                    .uncompressed_size = data.len,
                    .compressed_size = data.len,
                    .data = data,
                };
            },
            .LZ4 => {
                const compressed = try lz4.compress(allocator, data);
                return CompressedData{
                    .method = .LZ4,
                    .uncompressed_size = data.len,
                    .compressed_size = compressed.len,
                    .data = compressed,
                };
            },
            .ZSTD => {
                const compressed = try zstd.compress(allocator, data, 3); // Default compression level
                return CompressedData{
                    .method = .ZSTD,
                    .uncompressed_size = data.len,
                    .compressed_size = compressed.len,
                    .data = compressed,
                };
            },
        }
    }

    pub fn decompress(self: CompressedData, allocator: Allocator) ![]u8 {
        switch (self.method) {
            .None => {
                return allocator.dupe(u8, self.data);
            },
            .LZ4 => {
                return lz4.decompress(allocator, self.data, self.uncompressed_size);
            },
            .ZSTD => {
                return zstd.decompress(allocator, self.data, self.uncompressed_size);
            },
        }
    }

    pub fn deinit(self: *CompressedData, allocator: Allocator) void {
        if (self.method != .None) {
            allocator.free(self.data);
        }
    }
};