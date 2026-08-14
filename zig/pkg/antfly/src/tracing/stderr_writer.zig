// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const platform_sync = @import("antfly_platform").sync;
const raft_engine = @import("raft_engine");
const antfly_trace_writer = @import("antfly_trace_writer.zig");
const protocol_trace_writer = @import("protocol_trace_writer.zig");
const raft_trace_logger = @import("raft_trace_logger.zig");

/// A std.Io.Writer backed by libc write(2) to a trace output fd.
///
/// When `ANTFLY_TRACE_FILE` is set, output goes to that file (truncated on
/// first open). Otherwise output goes to stderr (fd 2). The Zig test runner
/// reserves stdout for `--listen` IPC, so direct fd-level writes are used.
const trace_vtable: std.Io.Writer.VTable = .{
    .drain = drain,
    .flush = flush,
};

var antfly_trace_buf: [4096]u8 = undefined;
var raft_trace_buf: [4096]u8 = undefined;
var protocol_trace_buf: [4096]u8 = undefined;

var antfly_trace_writer_instance: std.Io.Writer = .{
    .buffer = &antfly_trace_buf,
    .vtable = &trace_vtable,
};
var raft_trace_writer_instance: std.Io.Writer = .{
    .buffer = &raft_trace_buf,
    .vtable = &trace_vtable,
};
var protocol_trace_writer_instance: std.Io.Writer = .{
    .buffer = &protocol_trace_buf,
    .vtable = &protocol_trace_vtable,
};

var trace_fd: std.c.fd_t = -1;
var trace_fd_is_file = false;
var trace_output_mutex: std.atomic.Mutex = .unlocked;

const protocol_trace_vtable: std.Io.Writer.VTable = .{
    .drain = protocolDrain,
    .flush = protocolFlush,
};

fn getTraceFd() std.c.fd_t {
    if (trace_fd >= 0) return trace_fd;
    trace_fd_is_file = false;

    // Check ANTFLY_TRACE_FILE environment variable
    const path = std.c.getenv("ANTFLY_TRACE_FILE");
    if (path != null) {
        const fd = std.c.open(path.?, .{ .ACCMODE = .WRONLY, .CREAT = true, .TRUNC = true }, @as(std.c.mode_t, 0o644));
        if (fd >= 0) {
            trace_fd = fd;
            trace_fd_is_file = true;
            return trace_fd;
        }
    }

    // Default to stderr
    trace_fd = std.posix.STDERR_FILENO;
    return trace_fd;
}

fn drain(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
    platform_sync.lockYielding(&trace_output_mutex);
    defer trace_output_mutex.unlock();
    if (w.end > 0) {
        writeAllFd(w.buffer[0..w.end]);
        w.end = 0;
    }
    const pattern = data[data.len - 1];
    var written: usize = 0;
    for (data[0 .. data.len - 1]) |bytes| {
        writeAllFd(bytes);
        written += bytes.len;
    }
    writeAllFd(pattern);
    written += pattern.len;
    if (splat == 0) {
        // Pattern was written once but shouldn't have been; can't undo.
    } else {
        for (1..splat) |_| {
            writeAllFd(pattern);
            written += pattern.len;
        }
    }
    return written;
}

fn flush(w: *std.Io.Writer) std.Io.Writer.Error!void {
    platform_sync.lockYielding(&trace_output_mutex);
    defer trace_output_mutex.unlock();
    if (w.end > 0) {
        writeAllFd(w.buffer[0..w.end]);
        w.end = 0;
    }
}

fn writeAllFd(data: []const u8) void {
    const fd = getTraceFd();
    var offset: usize = 0;
    while (offset < data.len) {
        const rc = std.c.write(fd, data[offset..].ptr, data.len - offset);
        if (rc < 0) return;
        offset += @intCast(rc);
    }
}

fn protocolTraceEnabled(
    family: []const u8,
    path: ?[]const u8,
    families: ?[]const u8,
) bool {
    const trace_path = path orelse return false;
    if (trace_path.len == 0) return false;
    const family_list = families orelse return false;
    var tokens = std.mem.splitScalar(u8, family_list, ',');
    while (tokens.next()) |raw_token| {
        const token = std.mem.trim(u8, raw_token, &std.ascii.whitespace);
        if (std.mem.eql(u8, token, family)) return true;
    }
    return false;
}

fn getProtocolTraceFd() ?std.c.fd_t {
    if (trace_fd >= 0) return if (trace_fd_is_file) trace_fd else null;
    const path_z = std.c.getenv("ANTFLY_TRACE_FILE") orelse return null;
    const path = std.mem.span(path_z);
    if (path.len == 0) return null;
    const fd = std.c.open(path_z, .{ .ACCMODE = .WRONLY, .CREAT = true, .TRUNC = true }, @as(std.c.mode_t, 0o644));
    if (fd < 0) return null;
    trace_fd = fd;
    trace_fd_is_file = true;
    return fd;
}

fn protocolDrain(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
    platform_sync.lockYielding(&trace_output_mutex);
    defer trace_output_mutex.unlock();
    if (w.end > 0) {
        writeAllProtocolFd(w.buffer[0..w.end]);
        w.end = 0;
    }
    const pattern = data[data.len - 1];
    var written: usize = 0;
    for (data[0 .. data.len - 1]) |bytes| {
        writeAllProtocolFd(bytes);
        written += bytes.len;
    }
    writeAllProtocolFd(pattern);
    written += pattern.len;
    if (splat == 0) {
        // Pattern was written once but shouldn't have been; can't undo.
    } else {
        for (1..splat) |_| {
            writeAllProtocolFd(pattern);
            written += pattern.len;
        }
    }
    return written;
}

fn protocolFlush(w: *std.Io.Writer) std.Io.Writer.Error!void {
    platform_sync.lockYielding(&trace_output_mutex);
    defer trace_output_mutex.unlock();
    if (w.end > 0) {
        writeAllProtocolFd(w.buffer[0..w.end]);
        w.end = 0;
    }
}

fn writeAllProtocolFd(data: []const u8) void {
    const fd = getProtocolTraceFd() orelse return;
    var offset: usize = 0;
    while (offset < data.len) {
        const rc = std.c.write(fd, data[offset..].ptr, data.len - offset);
        if (rc <= 0) return;
        offset += @intCast(rc);
    }
}

/// Module-level singleton trace writer for Antfly transaction events.
pub fn stderrAntflyTraceWriter() antfly_trace_writer.AntflyTraceWriter {
    const S = struct {
        var ndjson_writer: antfly_trace_writer.AntflyNdjsonTraceWriter = .{ .writer = &antfly_trace_writer_instance };
    };
    return S.ndjson_writer.traceWriter();
}

/// Module-level singleton trace logger for Raft events.
pub fn stderrRaftTraceLogger() raft_engine.core.TraceLogger {
    const S = struct {
        var ndjson_logger: raft_trace_logger.RaftNdjsonTraceLogger = .{ .writer = &raft_trace_writer_instance };
    };
    return S.ndjson_logger.traceLogger();
}

/// Module-level singleton writer for explicitly enabled protocol event families.
pub fn stderrProtocolTraceWriter(family: []const u8) ?protocol_trace_writer.ProtocolTraceWriter {
    const path_z = std.c.getenv("ANTFLY_TRACE_FILE");
    const families_z = std.c.getenv("ANTFLY_TRACE_FAMILIES");
    if (!protocolTraceEnabled(
        family,
        if (path_z) |value| std.mem.span(value) else null,
        if (families_z) |value| std.mem.span(value) else null,
    )) return null;

    platform_sync.lockYielding(&trace_output_mutex);
    defer trace_output_mutex.unlock();
    _ = getProtocolTraceFd() orelse return null;

    const S = struct {
        var ndjson_writer: protocol_trace_writer.ProtocolNdjsonTraceWriter = .{ .writer = &protocol_trace_writer_instance };
    };
    return S.ndjson_writer.traceWriter();
}

test "protocol trace gate matches exact family token" {
    try std.testing.expect(protocolTraceEnabled(
        "derived-replay",
        "/tmp/protocol.ndjson",
        "placement-readiness,derived-replay",
    ));
}

test "protocol trace gate trims ASCII whitespace around family tokens" {
    try std.testing.expect(protocolTraceEnabled(
        "placement-readiness",
        "/tmp/protocol.ndjson",
        " index-lifecycle,\tplacement-readiness \r\n",
    ));
}

test "protocol trace gate rejects partial family tokens and implicit all" {
    try std.testing.expect(!protocolTraceEnabled(
        "derived-replay",
        "/tmp/protocol.ndjson",
        "derived-replay-extra",
    ));
    try std.testing.expect(!protocolTraceEnabled(
        "derived-replay",
        "/tmp/protocol.ndjson",
        "all",
    ));
}

test "protocol trace gate requires family list" {
    try std.testing.expect(!protocolTraceEnabled(
        "derived-replay",
        "/tmp/protocol.ndjson",
        null,
    ));
    try std.testing.expect(!protocolTraceEnabled(
        "derived-replay",
        "/tmp/protocol.ndjson",
        "",
    ));
}

test "protocol trace gate requires nonempty trace path" {
    try std.testing.expect(!protocolTraceEnabled(
        "derived-replay",
        null,
        "derived-replay",
    ));
    try std.testing.expect(!protocolTraceEnabled(
        "derived-replay",
        "",
        "derived-replay",
    ));
}

test "legacy and protocol writers share one trace file descriptor" {
    const c = struct {
        extern fn setenv(name: [*:0]const u8, value: [*:0]const u8, overwrite: c_int) c_int;
        extern fn unsetenv(name: [*:0]const u8) c_int;
        extern fn close(fd: c_int) c_int;
    };
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-shared-trace-writer-test.ndjson";
    const previous_path = if (std.c.getenv("ANTFLY_TRACE_FILE")) |value|
        try alloc.dupeZ(u8, std.mem.span(value))
    else
        null;
    defer {
        if (previous_path) |value| {
            _ = c.setenv("ANTFLY_TRACE_FILE", value.ptr, 1);
            alloc.free(value);
        } else {
            _ = c.unsetenv("ANTFLY_TRACE_FILE");
        }
    }
    std.Io.Dir.deleteFileAbsolute(std.testing.io, path) catch {};
    defer std.Io.Dir.deleteFileAbsolute(std.testing.io, path) catch {};
    try std.testing.expectEqual(@as(c_int, 0), c.setenv("ANTFLY_TRACE_FILE", path, 1));
    defer {
        if (trace_fd >= 0 and trace_fd != std.posix.STDERR_FILENO) {
            _ = c.close(trace_fd);
        }
        trace_fd = -1;
        trace_fd_is_file = false;
    }

    const legacy_fd = getTraceFd();
    writeAllFd("legacy\n");
    const protocol_fd = getProtocolTraceFd() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(legacy_fd, protocol_fd);
    writeAllProtocolFd("protocol\n");

    const output = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, path, alloc, .limited(64));
    defer alloc.free(output);
    try std.testing.expectEqualStrings("legacy\nprotocol\n", output);
}
