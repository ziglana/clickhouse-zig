const std = @import("std");
const ssl = @cImport({
    @cInclude("openssl/ssl.h");
    @cInclude("openssl/err.h");
});

pub const TlsConfig = struct {
    cert_path: ?[]const u8 = null,
    key_path: ?[]const u8 = null,
    ca_path: ?[]const u8 = null,
    verify_peer: bool = true,
    verify_host: bool = true,
    protocols: []const []const u8 = &[_][]const u8{"TLSv1.2", "TLSv1.3"},

    pub fn init() TlsConfig {
        return .{};
    }
};

pub const TlsContext = struct {
    ssl_ctx: *ssl.SSL_CTX,
    ssl: *ssl.SSL,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, config: TlsConfig) !*TlsContext {
        var ctx = try allocator.create(TlsContext);
        errdefer allocator.destroy(ctx);

        ssl.SSL_library_init();
        ssl.SSL_load_error_strings();

        const method = ssl.TLS_client_method();
        ctx.ssl_ctx = ssl.SSL_CTX_new(method) orelse return error.SslContextCreateFailed;
        errdefer ssl.SSL_CTX_free(ctx.ssl_ctx);

        if (config.verify_peer) {
            ssl.SSL_CTX_set_verify(ctx.ssl_ctx, ssl.SSL_VERIFY_PEER, null);
        }

        if (config.ca_path) |ca| {
            if (ssl.SSL_CTX_load_verify_locations(ctx.ssl_ctx, ca.ptr, null) != 1) {
                return error.LoadCaFailed;
            }
        }

        if (config.cert_path) |cert| {
            if (ssl.SSL_CTX_use_certificate_file(ctx.ssl_ctx, cert.ptr, ssl.SSL_FILETYPE_PEM) != 1) {
                return error.LoadCertFailed;
            }
        }

        if (config.key_path) |key| {
            if (ssl.SSL_CTX_use_PrivateKey_file(ctx.ssl_ctx, key.ptr, ssl.SSL_FILETYPE_PEM) != 1) {
                return error.LoadKeyFailed;
            }
        }

        ctx.ssl = ssl.SSL_new(ctx.ssl_ctx) orelse return error.SslCreateFailed;
        ctx.allocator = allocator;

        return ctx;
    }

    pub fn deinit(self: *TlsContext) void {
        ssl.SSL_free(self.ssl);
        ssl.SSL_CTX_free(self.ssl_ctx);
        self.allocator.destroy(self);
    }

    pub fn connect(self: *TlsContext, stream: std.net.Stream) !void {
        ssl.SSL_set_fd(self.ssl, @intCast(c_int, stream.handle));
        
        if (ssl.SSL_connect(self.ssl) != 1) {
            return error.TlsConnectFailed;
        }
    }

    pub fn read(self: *TlsContext, buffer: []u8) !usize {
        const result = ssl.SSL_read(self.ssl, buffer.ptr, @intCast(c_int, buffer.len));
        if (result <= 0) {
            const err = ssl.SSL_get_error(self.ssl, result);
            return switch (err) {
                ssl.SSL_ERROR_WANT_READ => error.WouldBlock,
                ssl.SSL_ERROR_WANT_WRITE => error.WouldBlock,
                ssl.SSL_ERROR_ZERO_RETURN => error.ConnectionClosed,
                else => error.TlsReadFailed,
            };
        }
        return @intCast(usize, result);
    }

    pub fn write(self: *TlsContext, buffer: []const u8) !usize {
        const result = ssl.SSL_write(self.ssl, buffer.ptr, @intCast(c_int, buffer.len));
        if (result <= 0) {
            const err = ssl.SSL_get_error(self.ssl, result);
            return switch (err) {
                ssl.SSL_ERROR_WANT_READ => error.WouldBlock,
                ssl.SSL_ERROR_WANT_WRITE => error.WouldBlock,
                ssl.SSL_ERROR_ZERO_RETURN => error.ConnectionClosed,
                else => error.TlsWriteFailed,
            };
        }
        return @intCast(usize, result);
    }
};