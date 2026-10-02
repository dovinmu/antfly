// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

const std = @import("std");
const pdf = @import("antfly_pdf");
const cli = @import("mod.zig");

pub const usage =
    \\usage: antfly pdf render-page <path|-> [options]
    \\
    \\  --page <n>             One-based page number (default: 1)
    \\  --dpi <72..600>         Requested raster resolution (default: 200)
    \\  --profile exact|ocr    Sampling profile (default: ocr)
    \\  --max-pixels <n>       Pixel safety cap (default: 40000000)
    \\  --max-dimension <n>    Dimension safety cap (default: 4096)
    \\  --require-native       Reject unsupported native rendering; no platform fallback
    \\  --out <path|->         PNG destination (default: stdout)
    \\Input is limited to 512 MiB. Geometry and render quality are reported on stderr.
    \\
;

pub fn run(alloc: std.mem.Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const subcommand = args.next() orelse return error.InvalidArguments;
    if (!std.mem.eql(u8, subcommand, "render-page")) return error.InvalidArguments;
    var source: ?[]const u8 = null;
    var output: []const u8 = "-";
    var page: usize = 1;
    var dpi: u16 = 200;
    var max_pixels: u64 = 40_000_000;
    var max_dimension: u32 = 4096;
    var profile: pdf.RenderProfile = .ocr;
    var require_native = false;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--require-native")) {
            require_native = true;
        } else if (std.mem.eql(u8, arg, "--page")) {
            page = try std.fmt.parseInt(usize, args.next() orelse return error.InvalidArguments, 10);
        } else if (std.mem.eql(u8, arg, "--dpi")) {
            dpi = try std.fmt.parseInt(u16, args.next() orelse return error.InvalidArguments, 10);
        } else if (std.mem.eql(u8, arg, "--max-pixels")) {
            max_pixels = try std.fmt.parseInt(u64, args.next() orelse return error.InvalidArguments, 10);
        } else if (std.mem.eql(u8, arg, "--max-dimension")) {
            max_dimension = try std.fmt.parseInt(u32, args.next() orelse return error.InvalidArguments, 10);
        } else if (std.mem.eql(u8, arg, "--profile")) {
            profile = std.meta.stringToEnum(pdf.RenderProfile, args.next() orelse return error.InvalidArguments) orelse return error.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--out")) {
            output = args.next() orelse return error.InvalidArguments;
        } else if (source == null and (!std.mem.startsWith(u8, arg, "-") or std.mem.eql(u8, arg, "-"))) {
            source = arg;
        } else return error.InvalidArguments;
    }
    if (page == 0 or dpi < 72 or dpi > 600 or max_pixels == 0 or max_pixels > 40_000_000 or max_dimension == 0 or max_dimension > 4096) return error.InvalidArguments;
    const path = source orelse return error.InvalidArguments;
    const max_input_bytes = 512 * 1024 * 1024;
    const bytes = if (std.mem.eql(u8, path, "-")) blk: {
        var buffer: [8192]u8 = undefined;
        var input = std.Io.File.stdin().readerStreaming(io, &buffer);
        break :blk try input.interface.allocRemaining(alloc, .limited(max_input_bytes));
    } else try cli.readFileAlloc(io, alloc, path, max_input_bytes);
    defer alloc.free(bytes);
    var parsed = try pdf.reader.Reader.init(alloc, bytes);
    defer parsed.deinit();
    var rendered = if (require_native)
        try pdf.renderParsedPagePngNativeAdaptiveWithProfileAlloc(alloc, &parsed, page, dpi, max_pixels, max_dimension, profile)
    else
        try pdf.renderParsedPagePngAdaptiveWithProfileAlloc(alloc, &parsed, page, dpi, max_pixels, max_dimension, profile);
    defer rendered.deinit(alloc);
    if (std.mem.eql(u8, output, "-")) {
        try std.Io.File.stdout().writeStreamingAll(io, rendered.png);
    } else {
        const file = try std.Io.Dir.cwd().createFile(io, output, .{});
        defer file.close(io);
        try file.writeStreamingAll(io, rendered.png);
    }
    const report = try std.json.Stringify.valueAlloc(alloc, .{
        .page = page,
        .requested_dpi = rendered.requested_dpi,
        .effective_dpi = rendered.effective_dpi,
        .width = rendered.width,
        .height = rendered.height,
        .quality = @tagName(rendered.quality),
        .diagnostics = rendered.diagnostics,
    }, .{});
    defer alloc.free(report);
    try std.Io.File.stderr().writeStreamingAll(io, report);
    try std.Io.File.stderr().writeStreamingAll(io, "\n");
}
