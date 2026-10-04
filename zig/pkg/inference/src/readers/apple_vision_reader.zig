// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! OCR reader backed by Apple's Vision framework (VNRecognizeTextRequest).
//!
//! The model directory holds no weights: its `antfly_metadata.json` declares
//! `"pipeline_type": "apple_vision_ocr"` and the operating system supplies the
//! recognizer. Results carry one region per recognized line, in reading order,
//! with pixel boxes relative to the decoded input image.

const std = @import("std");
const builtin = @import("builtin");
const build_options = @import("build_options");
const metadata_mod = @import("multistage_metadata.zig");
const reader_types = @import("types.zig");

pub const pipeline_type = "apple_vision_ocr";
pub const enabled = build_options.enable_apple_vision and builtin.os.tag == .macos;

/// Vision requests in flight per batch. Recognition runs on the Neural Engine
/// and GPU, so a few concurrent requests saturate it.
const max_concurrency: usize = 4;

pub fn isAppleVisionModelDir(allocator: std.mem.Allocator, model_dir: []const u8) !bool {
    var metadata = metadata_mod.loadFromDir(allocator, model_dir) catch |err| switch (err) {
        error.FileNotFound, error.NotDir => return false,
        else => return err,
    };
    defer metadata.deinit();
    const declared = metadata.pipeline_type orelse return false;
    return std.mem.eql(u8, declared, pipeline_type);
}

const c = struct {
    const Image = extern struct { bytes: [*]const u8, len: usize };
    const Line = extern struct { offset: usize, len: usize, bbox: [4]f64, confidence: f32 };
    const Result = extern struct {
        text: ?[*]u8,
        text_len: usize,
        lines: ?[*]Line,
        line_count: usize,
        width: f64,
        height: f64,
        status: i32,
    };
    extern fn antfly_vision_recognize_batch(images: [*]const Image, count: usize, max_concurrency: usize, results: [*]Result) i32;
    extern fn antfly_vision_free_results(results: [*]Result, count: usize) void;
};

pub const LoadedAppleVisionReader = struct {
    allocator: std.mem.Allocator,

    pub fn load(allocator: std.mem.Allocator) !LoadedAppleVisionReader {
        if (comptime !enabled) return error.AppleVisionUnavailable;
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *LoadedAppleVisionReader) void {
        self.* = undefined;
    }

    pub fn read(self: *LoadedAppleVisionReader, image_data: []const u8, options: reader_types.ReadOptions) !reader_types.Result {
        const results = try self.readBatch(&.{image_data}, options);
        defer self.allocator.free(results);
        return results[0];
    }

    /// Prompts and token limits do not apply: Vision returns every line it
    /// recognizes. Cancellation is checked before and after the batch.
    pub fn readBatch(self: *LoadedAppleVisionReader, image_datas: []const []const u8, options: reader_types.ReadOptions) ![]reader_types.Result {
        if (comptime !enabled) return error.AppleVisionUnavailable;
        if (options.execution_control) |control| try control.check();
        const images = try self.allocator.alloc(c.Image, image_datas.len);
        defer self.allocator.free(images);
        for (image_datas, images) |data, *image| image.* = .{ .bytes = data.ptr, .len = data.len };
        const raw = try self.allocator.alloc(c.Result, image_datas.len);
        defer self.allocator.free(raw);
        const status = c.antfly_vision_recognize_batch(images.ptr, images.len, max_concurrency, raw.ptr);
        defer c.antfly_vision_free_results(raw.ptr, raw.len);
        switch (status) {
            0 => {},
            1 => return error.InvalidImage,
            3 => return error.OutOfMemory,
            else => return error.AppleVisionRecognitionFailed,
        }
        if (options.execution_control) |control| try control.check();

        const results = try self.allocator.alloc(reader_types.Result, raw.len);
        var filled: usize = 0;
        errdefer {
            for (results[0..filled]) |*result| result.deinit();
            self.allocator.free(results);
        }
        for (raw, results) |item, *result| {
            result.* = try self.copyResult(item);
            filled += 1;
        }
        return results;
    }

    fn copyResult(self: *LoadedAppleVisionReader, item: c.Result) !reader_types.Result {
        const text_bytes: []const u8 = if (item.text) |ptr| ptr[0..item.text_len] else "";
        const lines: []const c.Line = if (item.lines) |ptr| ptr[0..item.line_count] else &.{};
        const text = try self.allocator.dupe(u8, text_bytes);
        errdefer self.allocator.free(text);
        const regions = try self.allocator.alloc(reader_types.Region, lines.len);
        var filled: usize = 0;
        errdefer {
            for (regions[0..filled]) |*region| region.deinit(self.allocator);
            self.allocator.free(regions);
        }
        for (lines, regions) |line, *region| {
            if (line.offset > text_bytes.len or line.len > text_bytes.len - line.offset) return error.AppleVisionRecognitionFailed;
            region.* = .{
                .text = try self.allocator.dupe(u8, text_bytes[line.offset..][0..line.len]),
                .bbox = line.bbox,
                .confidence = line.confidence,
                .coordinate_space = .image_pixels_top_left,
            };
            filled += 1;
        }
        return .{ .text = text, .regions = regions, .allocator = self.allocator };
    }
};

test "apple vision model directories are identified by pipeline type" {
    const allocator = std.testing.allocator;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    const path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", dir.sub_path[0..] });
    defer allocator.free(path);
    try std.testing.expect(!try isAppleVisionModelDir(allocator, path));

    try dir.dir.writeFile(std.testing.io, .{ .sub_path = "antfly_metadata.json", .data = "{\"pipeline_type\":\"apple_vision_ocr\"}" });
    try std.testing.expect(try isAppleVisionModelDir(allocator, path));

    try dir.dir.writeFile(std.testing.io, .{ .sub_path = "antfly_metadata.json", .data = "{\"pipeline_type\":\"multistage_ocr\"}" });
    try std.testing.expect(!try isAppleVisionModelDir(allocator, path));
}

test "apple vision reader rejects undecodable images" {
    if (comptime !enabled) return error.SkipZigTest;
    var reader = try LoadedAppleVisionReader.load(std.testing.allocator);
    defer reader.deinit();
    try std.testing.expectError(error.InvalidImage, reader.read("not an image", .{}));
}
