const std = @import("std");

pub const ProfileInfo = struct {
    rows: u64 = 0,
    blocks: u64 = 0,
    bytes: u64 = 0,
    applied_limit: bool = false,
    rows_before_limit: u64 = 0,
    calculated_rows_before_limit: bool = false,

    pub fn read(reader: anytype) !ProfileInfo {
        return ProfileInfo{
            .rows = try reader.readIntLittle(u64),
            .blocks = try reader.readIntLittle(u64),
            .bytes = try reader.readIntLittle(u64),
            .applied_limit = (try reader.readByte()) != 0,
            .rows_before_limit = try reader.readIntLittle(u64),
            .calculated_rows_before_limit = (try reader.readByte()) != 0,
        };
    }

    pub fn merge(self: *ProfileInfo, other: ProfileInfo) void {
        self.rows += other.rows;
        self.blocks += other.blocks;
        self.bytes += other.bytes;
        self.applied_limit = self.applied_limit or other.applied_limit;
        self.rows_before_limit += other.rows_before_limit;
        self.calculated_rows_before_limit = self.calculated_rows_before_limit or other.calculated_rows_before_limit;
    }
};