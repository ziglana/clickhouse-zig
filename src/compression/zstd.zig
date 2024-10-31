const std = @import("std");
const c = @cImport({
    @cInclude("zstd.h");
});

pub fn compress(allocator: std.mem.Allocator, data: []const u8, level: c_int) ![]u8 {
    const max_dst_size = c.ZSTD_compressBound(data.len);
    var compressed = try allocator.alloc(u8, max_dst_size);
    errdefer allocator.free(compressed);

    const compressed_size = c.ZSTD_compress(
        compressed.ptr,
        compressed.len,
        data.ptr,
        data.len,
        level,
    );

    if (c.ZSTD_isError(compressed_size) != 0) {
        return error.CompressionFailed;
    }

    return allocator.realloc(compressed, compressed_size);
}

pub fn decompress(allocator: std.mem.Allocator, compressed: []const u8, original_size: usize) ![]u8 {
    var decompressed = try allocator.alloc(u8, original_size);
    errdefer allocator.free(decompressed);

    const decompressed_size = c.ZSTD_decompress(
        decompressed.ptr,
        decompressed.len,
        compressed.ptr,
        compressed.len,
    );

    if (c.ZSTD_isError(decompressed_size) != 0) {
        return error.DecompressionFailed;
    }

    if (decompressed_size != original_size) {
        return error.SizeMismatch;
    }

    return decompressed;
}