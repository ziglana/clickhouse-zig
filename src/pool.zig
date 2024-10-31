const std = @import("std");
const ClickHouseClient = @import("main.zig").ClickHouseClient;
const ClickHouseConfig = @import("main.zig").ClickHouseConfig;

pub const ConnectionStrategy = enum {
    Lazy,   // Create connections as needed
    Eager,  // Create min_connections immediately
};

pub const PoolConfig = struct {
    min_connections: usize = 2,
    max_connections: usize = 10,
    connection_timeout_ms: u64 = 5000,
    max_idle_time_ms: u64 = 300000, // 5 minutes
    health_check_interval_ms: u64 = 30000, // 30 seconds
    retry_interval_ms: u64 = 1000,
    keep_alive_interval_ms: u64 = 60000, // 1 minute
    connection_strategy: ConnectionStrategy = .Lazy,
    max_wait_queue_size: usize = 100,
};

const ConnectionState = enum {
    Idle,
    InUse,
    Broken,
    Closed,
};

pub const Pool = struct {
    allocator: std.mem.Allocator,
    config: ClickHouseConfig,
    pool_config: PoolConfig,
    connections: std.ArrayList(*Connection),
    mutex: std.Thread.Mutex,
    health_check_timer: ?std.time.Timer,
    keep_alive_timer: ?std.time.Timer,
    shutdown: std.atomic.Atomic(bool),
    wait_queue: WaitQueue,

    const WaitQueue = struct {
        items: std.ArrayList(WaitItem),
        mutex: std.Thread.Mutex,

        const WaitItem = struct {
            completion: std.Thread.Condition,
            client: ?*ClickHouseClient,
            timeout_ms: u64,
            timestamp: i64,
        };

        fn init(allocator: std.mem.Allocator) WaitQueue {
            return .{
                .items = std.ArrayList(WaitItem).init(allocator),
                .mutex = std.Thread.Mutex{},
            };
        }

        fn deinit(self: *WaitQueue) void {
            self.items.deinit();
        }

        fn add(self: *WaitQueue, timeout_ms: u64) !*WaitItem {
            self.mutex.lock();
            defer self.mutex.unlock();

            try self.items.append(.{
                .completion = std.Thread.Condition{},
                .client = null,
                .timeout_ms = timeout_ms,
                .timestamp = std.time.milliTimestamp(),
            });

            return &self.items.items[self.items.items.len - 1];
        }

        fn remove(self: *WaitQueue, item: *WaitItem) void {
            self.mutex.lock();
            defer self.mutex.unlock();

            for (self.items.items, 0..) |*wait_item, i| {
                if (wait_item == item) {
                    _ = self.items.swapRemove(i);
                    break;
                }
            }
        }
    };

    const Connection = struct {
        client: *ClickHouseClient,
        state: ConnectionState,
        last_used: i64,
        last_checked: i64,

        pub fn init(allocator: std.mem.Allocator, config: ClickHouseConfig) !*Connection {
            var conn = try allocator.create(Connection);
            var client = try allocator.create(ClickHouseClient);
            client.* = ClickHouseClient.init(allocator, config);
            
            conn.* = .{
                .client = client,
                .state = .Idle,
                .last_used = std.time.milliTimestamp(),
                .last_checked = std.time.milliTimestamp(),
            };
            
            try client.connect();
            return conn;
        }

        pub fn deinit(self: *Connection, allocator: std.mem.Allocator) void {
            self.client.deinit();
            allocator.destroy(self.client);
            allocator.destroy(self);
        }

        pub fn ping(self: *Connection) bool {
            if (self.state == .Broken or self.state == .Closed) return false;
            
            self.client.query("SELECT 1") catch {
                self.state = .Broken;
                return false;
            };
            
            self.last_checked = std.time.milliTimestamp();
            return true;
        }

        pub fn isHealthy(self: *Connection) bool {
            return self.state == .Idle or self.state == .InUse;
        }

        pub fn isExpired(self: *Connection, max_idle_time_ms: u64) bool {
            if (self.state == .InUse) return false;
            const now = std.time.milliTimestamp();
            return (now - self.last_used) > max_idle_time_ms;
        }

        pub fn needsHealthCheck(self: *Connection, health_check_interval_ms: u64) bool {
            const now = std.time.milliTimestamp();
            return (now - self.last_checked) > health_check_interval_ms;
        }
    };

    pub fn init(allocator: std.mem.Allocator, config: ClickHouseConfig, pool_config: PoolConfig) !*Pool {
        var pool = try allocator.create(Pool);
        pool.* = .{
            .allocator = allocator,
            .config = config,
            .pool_config = pool_config,
            .connections = std.ArrayList(*Connection).init(allocator),
            .mutex = std.Thread.Mutex{},
            .health_check_timer = null,
            .keep_alive_timer = null,
            .shutdown = std.atomic.Atomic(bool).init(false),
            .wait_queue = WaitQueue.init(allocator),
        };

        if (pool_config.connection_strategy == .Eager) {
            var i: usize = 0;
            while (i < pool_config.min_connections) : (i += 1) {
                try pool.createConnection();
            }
        }

        try pool.startHealthCheck();
        try pool.startKeepAlive();

        return pool;
    }

    fn createConnection(self: *Pool) !void {
        var conn = try Connection.init(self.allocator, self.config);
        try self.connections.append(conn);
    }

    fn startHealthCheck(self: *Pool) !void {
        self.health_check_timer = try std.time.Timer.start();
        
        const thread = try std.Thread.spawn(.{}, struct {
            fn run(p: *Pool) void {
                while (!p.shutdown.load(.Acquire)) {
                    std.time.sleep(p.pool_config.health_check_interval_ms * std.time.ns_per_ms);
                    p.performHealthCheck();
                }
            }
        }.run, .{self});
        thread.detach();
    }

    fn startKeepAlive(self: *Pool) !void {
        self.keep_alive_timer = try std.time.Timer.start();
        
        const thread = try std.Thread.spawn(.{}, struct {
            fn run(p: *Pool) void {
                while (!p.shutdown.load(.Acquire)) {
                    std.time.sleep(p.pool_config.keep_alive_interval_ms * std.time.ns_per_ms);
                    p.performKeepAlive();
                }
            }
        }.run, .{self});
        thread.detach();
    }

    fn performHealthCheck(self: *Pool) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var i: usize = 0;
        while (i < self.connections.items.len) {
            var conn = self.connections.items[i];
            
            if (conn.isExpired(self.pool_config.max_idle_time_ms)) {
                if (self.connections.items.len > self.pool_config.min_connections) {
                    _ = self.connections.swapRemove(i);
                    conn.deinit(self.allocator);
                    continue;
                }
            } else if (conn.needsHealthCheck(self.pool_config.health_check_interval_ms)) {
                if (!conn.ping()) {
                    self.tryReconnect(conn);
                }
            }
            
            i += 1;
        }
    }

    fn performKeepAlive(self: *Pool) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.connections.items) |conn| {
            if (conn.state == .Idle) {
                _ = conn.ping();
            }
        }
    }

    fn tryReconnect(self: *Pool, conn: *Connection) void {
        if (conn.state == .Closed) return;

        conn.state = .Broken;
        
        const thread = std.Thread.spawn(.{}, struct {
            fn run(c: *Connection, config: ClickHouseConfig) void {
                c.client.deinit();
                c.client.* = ClickHouseClient.init(c.client.allocator, config);
                c.client.connect() catch {
                    c.state = .Broken;
                    return;
                };
                c.state = .Idle;
                c.last_checked = std.time.milliTimestamp();
            }
        }.run, .{ conn, self.config }) catch {
            conn.state = .Closed;
            return;
        };
        thread.detach();
    }

    pub fn acquire(self: *Pool) !*ClickHouseClient {
        while (true) {
            self.mutex.lock();
            
            // Try to find an available connection
            for (self.connections.items) |conn| {
                if (conn.state == .Idle) {
                    conn.state = .InUse;
                    conn.last_used = std.time.milliTimestamp();
                    self.mutex.unlock();
                    return conn.client;
                }
            }

            // Create new connection if possible
            if (self.connections.items.len < self.pool_config.max_connections) {
                var conn = Connection.init(self.allocator, self.config) catch {
                    self.mutex.unlock();
                    return error.ConnectionFailed;
                };
                conn.state = .InUse;
                try self.connections.append(conn);
                self.mutex.unlock();
                return conn.client;
            }

            self.mutex.unlock();

            // Wait for available connection
            if (self.wait_queue.items.items.len >= self.pool_config.max_wait_queue_size) {
                return error.WaitQueueFull;
            }

            var wait_item = try self.wait_queue.add(self.pool_config.connection_timeout_ms);
            defer self.wait_queue.remove(wait_item);

            const start = std.time.milliTimestamp();
            while (true) {
                const now = std.time.milliTimestamp();
                if (now - start >= self.pool_config.connection_timeout_ms) {
                    return error.AcquireTimeout;
                }

                if (wait_item.client) |client| {
                    return client;
                }

                std.time.sleep(self.pool_config.retry_interval_ms * std.time.ns_per_ms);
            }
        }
    }

    pub fn release(self: *Pool, client: *ClickHouseClient) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.connections.items) |conn| {
            if (conn.client == client) {
                conn.state = .Idle;
                conn.last_used = std.time.milliTimestamp();

                // Check wait queue
                self.wait_queue.mutex.lock();
                defer self.wait_queue.mutex.unlock();

                if (self.wait_queue.items.items.len > 0) {
                    var wait_item = &self.wait_queue.items.items[0];
                    wait_item.client = client;
                    wait_item.completion.signal();
                }
                
                break;
            }
        }
    }

    pub fn deinit(self: *Pool) void {
        self.shutdown.store(true, .Release);
        
        for (self.connections.items) |conn| {
            conn.deinit(self.allocator);
        }
        self.connections.deinit();
        self.wait_queue.deinit();
        self.allocator.destroy(self);
    }
};