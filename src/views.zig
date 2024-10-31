const std = @import("std");
const ClickHouseClient = @import("main.zig").ClickHouseClient;

pub const MaterializedView = struct {
    name: []const u8,
    target_table: []const u8,
    query: []const u8,
    populate: bool,
    cluster: ?[]const u8,
    allocator: std.mem.Allocator,

    pub fn init(
        allocator: std.mem.Allocator,
        name: []const u8,
        target_table: []const u8,
        query: []const u8,
    ) !*MaterializedView {
        var view = try allocator.create(MaterializedView);
        view.* = .{
            .name = try allocator.dupe(u8, name),
            .target_table = try allocator.dupe(u8, target_table),
            .query = try allocator.dupe(u8, query),
            .populate = false,
            .cluster = null,
            .allocator = allocator,
        };
        return view;
    }

    pub fn deinit(self: *MaterializedView) void {
        self.allocator.free(self.name);
        self.allocator.free(self.target_table);
        self.allocator.free(self.query);
        if (self.cluster) |cluster| {
            self.allocator.free(cluster);
        }
        self.allocator.destroy(self);
    }

    pub fn setCluster(self: *MaterializedView, cluster: []const u8) !void {
        if (self.cluster) |old_cluster| {
            self.allocator.free(old_cluster);
        }
        self.cluster = try self.allocator.dupe(u8, cluster);
    }

    pub fn create(self: *MaterializedView, client: *ClickHouseClient) !void {
        var query_buf = std.ArrayList(u8).init(self.allocator);
        defer query_buf.deinit();

        try query_buf.writer().print(
            \\CREATE MATERIALIZED VIEW {s}
            \\TO {s}
        , .{ self.name, self.target_table });

        if (self.cluster) |cluster| {
            try query_buf.writer().print(" ON CLUSTER {s}", .{cluster});
        }

        if (self.populate) {
            try query_buf.appendSlice(" POPULATE");
        }

        try query_buf.writer().print(" AS {s}", .{self.query});

        try client.query(query_buf.items);
    }

    pub fn drop(self: *MaterializedView, client: *ClickHouseClient) !void {
        var query_buf = std.ArrayList(u8).init(self.allocator);
        defer query_buf.deinit();

        try query_buf.writer().print("DROP VIEW IF EXISTS {s}", .{self.name});
        if (self.cluster) |cluster| {
            try query_buf.writer().print(" ON CLUSTER {s}", .{cluster});
        }

        try client.query(query_buf.items);
    }

    pub fn refresh(self: *MaterializedView, client: *ClickHouseClient) !void {
        var query_buf = std.ArrayList(u8).init(self.allocator);
        defer query_buf.deinit();

        try query_buf.writer().print("ALTER VIEW {s} REFRESH", .{self.name});
        if (self.cluster) |cluster| {
            try query_buf.writer().print(" ON CLUSTER {s}", .{cluster});
        }

        try client.query(query_buf.items);
    }
};

pub const View = struct {
    name: []const u8,
    query: []const u8,
    cluster: ?[]const u8,
    allocator: std.mem.Allocator,

    pub fn init(
        allocator: std.mem.Allocator,
        name: []const u8,
        query: []const u8,
    ) !*View {
        var view = try allocator.create(View);
        view.* = .{
            .name = try allocator.dupe(u8, name),
            .query = try allocator.dupe(u8, query),
            .cluster = null,
            .allocator = allocator,
        };
        return view;
    }

    pub fn deinit(self: *View) void {
        self.allocator.free(self.name);
        self.allocator.free(self.query);
        if (self.cluster) |cluster| {
            self.allocator.free(cluster);
        }
        self.allocator.destroy(self);
    }

    pub fn setCluster(self: *View, cluster: []const u8) !void {
        if (self.cluster) |old_cluster| {
            self.allocator.free(old_cluster);
        }
        self.cluster = try self.allocator.dupe(u8, cluster);
    }

    pub fn create(self: *View, client: *ClickHouseClient) !void {
        var query_buf = std.ArrayList(u8).init(self.allocator);
        defer query_buf.deinit();

        try query_buf.writer().print("CREATE VIEW {s}", .{self.name});
        if (self.cluster) |cluster| {
            try query_buf.writer().print(" ON CLUSTER {s}", .{cluster});
        }
        try query_buf.writer().print(" AS {s}", .{self.query});

        try client.query(query_buf.items);
    }

    pub fn drop(self: *View, client: *ClickHouseClient) !void {
        var query_buf = std.ArrayList(u8).init(self.allocator);
        defer query_buf.deinit();

        try query_buf.writer().print("DROP VIEW IF EXISTS {s}", .{self.name});
        if (self.cluster) |cluster| {
            try query_buf.writer().print(" ON CLUSTER {s}", .{cluster});
        }

        try client.query(query_buf.items);
    }
};