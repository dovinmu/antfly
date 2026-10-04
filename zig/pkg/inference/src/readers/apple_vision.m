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

// Apple Vision text recognition for the apple_vision_ocr reader. The C ABI
// mirrors apple_vision_reader.zig: each encoded image yields one UTF-8 text
// buffer (recognized lines joined by '\n') plus per-line pixel boxes that
// index into it. Buffers are malloc-owned until antfly_vision_free_results.

#import <CoreGraphics/CoreGraphics.h>
#import <Foundation/Foundation.h>
#import <ImageIO/ImageIO.h>
#import <Vision/Vision.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    const uint8_t *bytes;
    size_t len;
} AntflyVisionImage;

typedef struct {
    size_t offset;
    size_t len;
    // [left, top, right, bottom] in pixels of the decoded image, origin top-left.
    double bbox[4];
    float confidence;
} AntflyVisionLine;

typedef struct {
    char *text;
    size_t text_len;
    AntflyVisionLine *lines;
    size_t line_count;
    double width;
    double height;
    int32_t status;
} AntflyVisionResult;

enum {
    ANTFLY_VISION_OK = 0,
    ANTFLY_VISION_DECODE_FAILED = 1,
    ANTFLY_VISION_RECOGNITION_FAILED = 2,
    ANTFLY_VISION_OUT_OF_MEMORY = 3,
};

static int32_t recognize_one(const AntflyVisionImage *image, AntflyVisionResult *out) {
    memset(out, 0, sizeof(*out));
    NSData *data = [NSData dataWithBytesNoCopy:(void *)image->bytes length:image->len freeWhenDone:NO];
    CGImageSourceRef source = CGImageSourceCreateWithData((__bridge CFDataRef)data, NULL);
    if (source == NULL) return ANTFLY_VISION_DECODE_FAILED;
    CGImageRef cg_image = CGImageSourceCreateImageAtIndex(source, 0, NULL);
    CFRelease(source);
    if (cg_image == NULL) return ANTFLY_VISION_DECODE_FAILED;

    const double width = (double)CGImageGetWidth(cg_image);
    const double height = (double)CGImageGetHeight(cg_image);
    VNRecognizeTextRequest *request = [[VNRecognizeTextRequest alloc] init];
    request.recognitionLevel = VNRequestTextRecognitionLevelAccurate;
    request.usesLanguageCorrection = YES;
    if (@available(macOS 13.0, *)) request.automaticallyDetectsLanguage = YES;
    VNImageRequestHandler *handler = [[VNImageRequestHandler alloc] initWithCGImage:cg_image options:@{}];
    NSError *error = nil;
    const BOOL performed = [handler performRequests:@[ request ] error:&error];
    CGImageRelease(cg_image);
    if (!performed) return ANTFLY_VISION_RECOGNITION_FAILED;

    NSArray<VNRecognizedTextObservation *> *observations = request.results ?: @[];
    NSMutableData *text = [NSMutableData data];
    AntflyVisionLine *lines = observations.count > 0 ? calloc(observations.count, sizeof(AntflyVisionLine)) : NULL;
    if (observations.count > 0 && lines == NULL) return ANTFLY_VISION_OUT_OF_MEMORY;
    size_t line_count = 0;
    for (VNRecognizedTextObservation *observation in observations) {
        VNRecognizedText *candidate = [[observation topCandidates:1] firstObject];
        NSData *utf8 = [candidate.string dataUsingEncoding:NSUTF8StringEncoding];
        if (utf8.length == 0) continue;
        if (line_count > 0) [text appendBytes:"\n" length:1];
        // Vision boxes are normalized with a bottom-left origin.
        const CGRect box = observation.boundingBox;
        lines[line_count] = (AntflyVisionLine){
            .offset = text.length,
            .len = utf8.length,
            .bbox = {
                box.origin.x * width,
                (1.0 - box.origin.y - box.size.height) * height,
                (box.origin.x + box.size.width) * width,
                (1.0 - box.origin.y) * height,
            },
            .confidence = candidate.confidence,
        };
        [text appendData:utf8];
        line_count += 1;
    }

    out->text = malloc(text.length > 0 ? text.length : 1);
    if (out->text == NULL) {
        free(lines);
        return ANTFLY_VISION_OUT_OF_MEMORY;
    }
    memcpy(out->text, text.bytes, text.length);
    out->text_len = text.length;
    out->lines = lines;
    out->line_count = line_count;
    out->width = width;
    out->height = height;
    return ANTFLY_VISION_OK;
}

// Recognizes every image with at most max_concurrency requests in flight.
// Returns the first nonzero per-image status, or ANTFLY_VISION_OK.
int32_t antfly_vision_recognize_batch(
    const AntflyVisionImage *images,
    size_t count,
    size_t max_concurrency,
    AntflyVisionResult *results
) {
    if (count == 0) return ANTFLY_VISION_OK;
    dispatch_semaphore_t slots = dispatch_semaphore_create((long)(max_concurrency > 0 ? max_concurrency : 1));
    dispatch_queue_t queue = dispatch_get_global_queue(QOS_CLASS_USER_INITIATED, 0);
    dispatch_apply(count, queue, ^(size_t index) {
        dispatch_semaphore_wait(slots, DISPATCH_TIME_FOREVER);
        @autoreleasepool {
            results[index].status = recognize_one(&images[index], &results[index]);
        }
        dispatch_semaphore_signal(slots);
    });
    for (size_t index = 0; index < count; index++) {
        if (results[index].status != ANTFLY_VISION_OK) return results[index].status;
    }
    return ANTFLY_VISION_OK;
}

void antfly_vision_free_results(AntflyVisionResult *results, size_t count) {
    for (size_t index = 0; index < count; index++) {
        free(results[index].text);
        free(results[index].lines);
        results[index].text = NULL;
        results[index].lines = NULL;
    }
}
