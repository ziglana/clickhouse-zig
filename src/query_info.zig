const std = @import("std");
const progress = @import("progress.zig");
const statistics = @import("statistics.zig");
const profile = @import("profile_info.zig");

pub const QueryInfo = struct {
    progress: progress.Progress,
    statistics: statistics.Statistics,
    profile: profile.ProfileInfo,
    written_rows: u64,
    written_bytes: u64,

    pub fn init() QueryInfo {
        return .{
            .progress = .{},
            .statistics = .{},
            .profile = .{},
            .written_rows = 0,
            .written_bytes = 0,
        };
    }

    pub fn updateProgress(self: *QueryInfo, new_progress: progress.Progress) void {
        self.progress.merge(new_progress);
    }

    pub fn updateStatistics(self: *QueryInfo, new_stats: statistics.Statistics) void {
        self.statistics.merge(new_stats);
    }

    pub fn updateProfile(self: *QueryInfo, new_profile: profile.ProfileInfo) void {
        self.profile.merge(new_profile);
    }

    pub fn updateWritten(self: *QueryInfo, rows: u64, bytes: u64) void {
        self.written_rows += rows;
        self.written_bytes += bytes;
    }

    pub fn totalRows(self: QueryInfo) u64 {
        return self.progress.rows + self.written_rows;
    }

    pub fn totalBytes(self: QueryInfo) u64 {
        return self.progress.bytes + self.written_bytes;
    }

    pub fn rowsPerSecond(self: QueryInfo) f64 {
        if (self.progress.elapsed_ns == 0) return 0;
        return @as(f64, @floatFromInt(self.totalRows())) / (@as(f64, @floatFromInt(self.progress.elapsed_ns)) / 1_000_000_000.0);
    }

    pub fn bytesPerSecond(self: QueryInfo) f64 {
        if (self.progress.elapsed_ns == 0) return 0;
        return @as(f64, @floatFromInt(self.totalBytes())) / (@as(f64, @floatFromInt(self.progress.elapsed_ns)) / 1_000_000_000.0);
    }
};