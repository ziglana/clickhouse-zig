const std = @import("std");
const c = @cImport({
    @cInclude("lz4.h");
});

pub fn compress(allocator: std.mem.Allocator, data: []const u8) ![]u8 {
    const max_dst_size = c.LZ4_compressBound(@intCast(c_int, data.len));
    var compressed = try allocator.alloc(u8, @intCast(usize, max_dst_size));
    errdefer allocator.free(compressed);

    const compressed_size = c.LZ4_compress_default(
        @ptrCast([*c]const u8, data.ptr),
        @ptrCast([*c]u8, compressed.ptr),
        @intCast(c_int, data.len),
        @intCast(c_int, compressed.len),
    );

    if (compressed_size <= 0) {
        return error.CompressionFailed;
    }

    return allocator.realloc(compressed, @intCast(usize, compressed_size));
}

pub fn decompress(allocator: std.mem.Allocator, compressed: []const u8, original_size: usize) ![]u8 {
    var decompressed = try allocator.alloc(u8, original_size);
    errdefer allocator.free(decompressed);

    const decompressed_size = c.LZ4_decompress_safe(
        @ptrCast([*c]const u8, compressed.ptr),
        @ptrCast([*c]u8, decompressed.ptr),
        @intCast(c_int, compressed.len),
        @intCast(c_int, decompressed.len),
    );

    if (decompressed_size < 0) {
        return error.DecompressionFailed;
    }

    if (decompressed_size != original_size) {
        return error.SizeMismatch;
    }

    return decompressed;
}