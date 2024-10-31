const std = @import("std");
const ClickHouseClient = @import("main.zig").ClickHouseClient;

pub const Dictionary = struct {
    name: []const u8,
    source: DictionarySource,
    layout: DictionaryLayout,
    lifetime: DictionaryLifetime,
    structure: []const DictionaryAttribute,
    allocator: std.mem.Allocator,

    pub const DictionarySource = union(enum) {
        ClickHouse: struct {
            db: []const u8,
            table: []const u8,
        },
        File: struct {
            path: []const u8,
            format: FileFormat,
        },
        MySQL: struct {
            host: []const u8,
            port: u16,
            db: []const u8,
            table: []const u8,
            user: []const u8,
            password: []const u8,
        },
        MongoDB: struct {
            uri: []const u8,
            db: []const u8,
            collection: []const u8,
        },

        pub const FileFormat = enum {
            CSV,
            TSV,
            JSONEachRow,
            Parquet,
        };
    };

    pub const DictionaryLayout = union(enum) {
        FlatArray,
        HashTable,
        ComplexKeyHashTable,
        DirectArray,
        IPTrie,
        RangeHashed: struct {
            range_min: []const u8,
            range_max: []const u8,
        },
    };

    pub const DictionaryLifetime = union(enum) {
        Default,
        TTL: u64,
        KeepAlive: struct {
            min_sec: u64,
            max_sec: u64,
        },
    };

    pub const DictionaryAttribute = struct {
        name: []const u8,
        type: []const u8,
        expression: ?[]const u8 = null,
        hierarchical: bool = false,
        injective: bool = false,
    };

    pub fn init(
        allocator: std.mem.Allocator,
        name: []const u8,
        source: DictionarySource,
        layout: DictionaryLayout,
        lifetime: DictionaryLifetime,
        structure: []const DictionaryAttribute,
    ) !*Dictionary {
        var dict = try allocator.create(Dictionary);
        dict.* = .{
            .name = try allocator.dupe(u8, name),
            .source = source,
            .layout = layout,
            .lifetime = lifetime,
            .structure = try allocator.dupe(DictionaryAttribute, structure),
            .allocator = allocator,
        };
        return dict;
    }

    pub fn deinit(self: *Dictionary) void {
        self.allocator.free(self.name);
        switch (self.source) {
            .ClickHouse => |ch| {
                self.allocator.free(ch.db);
                self.allocator.free(ch.table);
            },
            .File => |f| {
                self.allocator.free(f.path);
            },
            .MySQL => |mysql| {
                self.allocator.free(mysql.host);
                self.allocator.free(mysql.db);
                self.allocator.free(mysql.table);
                self.allocator.free(mysql.user);
                self.allocator.free(mysql.password);
            },
            .MongoDB => |mongo| {
                self.allocator.free(mongo.uri);
                self.allocator.free(mongo.db);
                self.allocator.free(mongo.collection);
            },
        }
        for (self.structure) |attr| {
            self.allocator.free(attr.name);
            self.allocator.free(attr.type);
            if (attr.expression) |expr| {
                self.allocator.free(expr);
            }
        }
        self.allocator.free(self.structure);
        self.allocator.destroy(self);
    }

    pub fn create(self: *Dictionary, client: *ClickHouseClient) !void {
        var query_buf = std.ArrayList(u8).init(self.allocator);
        defer query_buf.deinit();

        try query_buf.writer().print("CREATE DICTIONARY {s}\n(\n", .{self.name});

        // Add structure
        for (self.structure, 0..) |attr, i| {
            if (i > 0) try query_buf.appendSlice(",\n");
            try query_buf.writer().print("    {s} {s}", .{ attr.name, attr.type });
            if (attr.expression) |expr| {
                try query_buf.writer().print(" EXPRESSION {s}", .{expr});
            }
            if (attr.hierarchical) {
                try query_buf.appendSlice(" HIERARCHICAL");
            }
            if (attr.injective) {
                try query_buf.appendSlice(" INJECTIVE");
            }
        }

        // Add source
        try query_buf.appendSlice("\n)\nSOURCE(");
        switch (self.source) {
            .ClickHouse => |ch| try query_buf.writer().print(
                "CLICKHOUSE(DB '{s}' TABLE '{s}')",
                .{ ch.db, ch.table },
            ),
            .File => |f| try query_buf.writer().print(
                "FILE(PATH '{s}' FORMAT {s})",
                .{ f.path, @tagName(f.format) },
            ),
            .MySQL => |mysql| try query_buf.writer().print(
                \\MYSQL(
                \\    HOST '{s}'
                \\    PORT {d}
                \\    DB '{s}'
                \\    TABLE '{s}'
                \\    USER '{s}'
                \\    PASSWORD '{s}'
                \\)
            , .{
                mysql.host,
                mysql.port,
                mysql.db,
                mysql.table,
                mysql.user,
                mysql.password,
            }),
            .MongoDB => |mongo| try query_buf.writer().print(
                \\MONGODB(
                \\    URI '{s}'
                \\    DB '{s}'
                \\    COLLECTION '{s}'
                \\)
            , .{ mongo.uri, mongo.db, mongo.collection }),
        }

        // Add layout
        try query_buf.appendSlice(")\nLAYOUT(");
        switch (self.layout) {
            .FlatArray => try query_buf.appendSlice("FLAT"),
            .HashTable => try query_buf.appendSlice("HASHED"),
            .ComplexKeyHashTable => try query_buf.appendSlice("COMPLEX_KEY_HASHED"),
            .DirectArray => try query_buf.appendSlice("DIRECT"),
            .IPTrie => try query_buf.appendSlice("IP_TRIE"),
            .RangeHashed => |range| try query_buf.writer().print(
                "RANGE_HASHED(RANGE_MIN {s} RANGE_MAX {s})",
                .{ range.range_min, range.range_max },
            ),
        }

        // Add lifetime
        try query_buf.appendSlice(")\nLIFETIME(");
        switch (self.lifetime) {
            .Default => try query_buf.appendSlice("MIN 300 MAX 360"),
            .TTL => |ttl| try query_buf.writer().print("TTL {d}", .{ttl}),
            .KeepAlive => |ka| try query_buf.writer().print(
                "MIN {d} MAX {d}",
                .{ ka.min_sec, ka.max_sec },
            ),
        }
        try query_buf.appendSlice(")");

        try client.query(query_buf.items);
    }

    pub fn drop(self: *Dictionary, client: *ClickHouseClient) !void {
        var query_buf = std.ArrayList(u8).init(self.allocator);
        defer query_buf.deinit();

        try query_buf.writer().print("DROP DICTIONARY IF EXISTS {s}", .{self.name});
        try client.query(query_buf.items);
    }
};