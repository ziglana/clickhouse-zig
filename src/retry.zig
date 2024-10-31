const std = @import("std");

pub const RetryStrategy = struct {
    max_attempts: u32,
    initial_delay_ms: u64,
    max_delay_ms: u64,
    multiplier: f64,
    jitter: bool,

    pub fn init() RetryStrategy {
        return .{
            .max_attempts = 3,
            .initial_delay_ms = 100,
            .max_delay_ms = 10000,
            .multiplier = 2.0,
            .jitter = true,
        };
    }

    pub fn nextDelay(self: RetryStrategy, attempt: u32) u64 {
        if (attempt >= self.max_attempts) return 0;

        const base_delay = @floatToInt(u64, 
            @intToFloat(f64, self.initial_delay_ms) * std.math.pow(f64, self.multiplier, @intToFloat(f64, attempt))
        );

        const delay = @min(base_delay, self.max_delay_ms);

        if (!self.jitter) return delay;

        // Add random jitter between 0-20%
        var random = std.crypto.random;
        const jitter = random.intRangeAtMost(u64, 0, delay / 5);
        return delay + jitter;
    }
};

pub fn retry(
    comptime T: type,
    strategy: RetryStrategy,
    context: anytype,
    operation: fn(@TypeOf(context)) T!void,
) T!void {
    var attempt: u32 = 0;
    var last_error: T!void = undefined;

    while (attempt < strategy.max_attempts) : (attempt += 1) {
        operation(context) catch |err| {
            last_error = err;
            
            const delay = strategy.nextDelay(attempt);
            if (delay == 0) break;
            
            std.time.sleep(delay * std.time.ns_per_ms);
            continue;
        };
        
        return;
    }

    return last_error;
}