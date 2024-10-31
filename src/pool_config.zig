const std = @import("std");
const RetryStrategy = @import("retry.zig").RetryStrategy;

pub const PoolConfig = struct {
    min_connections: usize = 2,
    max_connections: usize = 10,
    connection_timeout_ms: u64 = 5000,
    max_idle_time_ms: u64 = 300000, // 5 minutes
    health_check_interval_ms: u64 = 30000, // 30 seconds
    retry_strategy: RetryStrategy = RetryStrategy.init(),
    max_wait_queue_size: usize = 100,
    keep_alive: bool = true,
    tcp_nodelay: bool = true,
    tcp_keepalive_time: u32 = 7200,
    tcp_keepalive_interval: u32 = 75,
    tcp_keepalive_count: u32 = 9,

    pub fn init() PoolConfig {
        return .{};
    }

    pub fn setMinConnections(self: *PoolConfig, min: usize) void {
        self.min_connections = min;
    }

    pub fn setMaxConnections(self: *PoolConfig, max: usize) void {
        self.max_connections = max;
    }

    pub fn setConnectionTimeout(self: *PoolConfig, timeout_ms: u64) void {
        self.connection_timeout_ms = timeout_ms;
    }

    pub fn setMaxIdleTime(self: *PoolConfig, idle_time_ms: u64) void {
        self.max_idle_time_ms = idle_time_ms;
    }

    pub fn setHealthCheckInterval(self: *PoolConfig, interval_ms: u64) void {
        self.health_check_interval_ms = interval_ms;
    }

    pub fn setRetryStrategy(self: *PoolConfig, strategy: RetryStrategy) void {
        self.retry_strategy = strategy;
    }

    pub fn setMaxWaitQueueSize(self: *PoolConfig, size: usize) void {
        self.max_wait_queue_size = size;
    }

    pub fn setTcpKeepalive(self: *PoolConfig, enable: bool) void {
        self.keep_alive = enable;
    }

    pub fn setTcpNodelay(self: *PoolConfig, enable: bool) void {
        self.tcp_nodelay = enable;
    }
};