const std = @import("std");

pub const PacketType = enum(u64) {
    Hello = 0,
    Data = 1,
    Query = 2,
    Error = 3,
    Progress = 4,
    Pong = 5,
    EndOfStream = 6,
};

pub fn writePacketHeader(writer: anytype, packet_type: PacketType) !void {
    try writer.writeIntLittle(u64, @intFromEnum(packet_type));
}