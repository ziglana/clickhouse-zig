const std = @import("std");
const block = @import("block.zig");
const results = @import("results.zig");

pub const StreamBuffer = struct {
    const default_capacity = 1000;
    const BlockNode = struct {
        block: *block.Block,
        next: ?*BlockNode,
    };

    allocator: std.mem.Allocator,
    head: ?*BlockNode,
    tail: ?*BlockNode,
    capacity: usize,
    size: usize,
    mutex: std.Thread.Mutex,
    not_empty: std.Thread.Condition,
    not_full: std.Thread.Condition,

    pub fn init(allocator: std.mem.Allocator) StreamBuffer {
        return .{
            .allocator = allocator,
            .head = null,
            .tail = null,
            .capacity = default_capacity,
            .size = 0,
            .mutex = std.Thread.Mutex{},
            .not_empty = std.Thread.Condition{},
            .not_full = std.Thread.Condition{},
        };
    }

    pub fn deinit(self: *StreamBuffer) void {
        var current = self.head;
        while (current) |node| {
            const next = node.next;
            node.block.deinit();
            self.allocator.destroy(node.block);
            self.allocator.destroy(node);
            current = next;
        }
    }

    pub fn push(self: *StreamBuffer, b: *block.Block) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        while (self.size >= self.capacity) {
            self.not_full.wait(&self.mutex);
        }

        var node = try self.allocator.create(BlockNode);
        node.block = b;
        node.next = null;

        if (self.tail) |tail| {
            tail.next = node;
            self.tail = node;
        } else {
            self.head = node;
            self.tail = node;
        }

        self.size += 1;
        self.not_empty.signal();
    }

    pub fn pop(self: *StreamBuffer) ?*block.Block {
        self.mutex.lock();
        defer self.mutex.unlock();

        while (self.size == 0) {
            self.not_empty.wait(&self.mutex);
        }

        if (self.head) |node| {
            const block = node.block;
            self.head = node.next;
            if (self.head == null) {
                self.tail = null;
            }
            self.allocator.destroy(node);
            self.size -= 1;
            self.not_full.signal();
            return block;
        }

        return null;
    }

    pub fn setCapacity(self: *StreamBuffer, capacity: usize) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.capacity = capacity;
        self.not_full.broadcast();
    }

    pub fn clear(self: *StreamBuffer) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var current = self.head;
        while (current) |node| {
            const next = node.next;
            node.block.deinit();
            self.allocator.destroy(node.block);
            self.allocator.destroy(node);
            current = next;
        }

        self.head = null;
        self.tail = null;
        self.size = 0;
        self.not_full.broadcast();
    }
};