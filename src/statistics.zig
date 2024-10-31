const std = @import("std");

pub const Statistics = struct {
    rows_read: u64 = 0,
    bytes_read: u64 = 0,
    total_rows_approx: u64 = 0,
    elapsed_ns: u64 = 0,

    pub fn read(reader: anytype) !Statistics {
        return Statistics{
            .rows_read = try reader.readIntLittle(u64),
            .bytes_read = try reader.readIntLittle(u64),
            .total_rows_approx = try reader.readIntLittle(u64),
            .elapsed_ns = try reader.readIntLittle(u64),
        };
    }

    pub fn merge(self: *Statistics, other: Statistics) void {
        self.rows_read += other.rows_read;
        self.bytes_read += other.bytes_read;
        self.total_rows_approx = other.total_rows_approx;
        self.elapsed_ns = other.elapsed_ns;
    }

    pub fn rowsPerSecond(self: Statistics) f64 {
        if (self.elapsed_ns == 0) return 0;
        return @as(f64, @floatFromInt(self.rows_read)) / (@as(f64, @floatFromInt(self.elapsed_ns)) / 1_000_000_000.0);
    }

    pub fn bytesPerSecond(self: Statistics) f64 {
        if (self.elapsed_ns == 0) return 0;
        return @as(f64, @floatFromInt(self.bytes_read)) / (@as(f64, @floatFromInt(self.elapsed_ns)) / 1_000_000_000.0);
    }
};