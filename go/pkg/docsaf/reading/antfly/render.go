package antfly

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"image/png"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/antflydb/antfly/go/pkg/docsaf/reading"
)

const (
	DefaultRenderDPI           = 150
	nativeRenderMaxPixels      = 40_000_000
	nativeRenderMaxDimension   = 4096
	nativeRenderMaxOutputBytes = 80 << 20
	nativeRenderMaxStderrBytes = 64 << 10
)

var errNativeRenderOutputTooLarge = errors.New("native PDF renderer output exceeds limit")

// RenderPDFPage renders a PDF page to PNG with the native Antfly PDF engine.
// ANTFLY_BIN must name the Antfly executable; there is deliberately no
// platform or external-renderer fallback.
func RenderPDFPage(pdfData []byte, pageNum int, dpi float64) (reading.BinaryContent, error) {
	return RenderPDFPageContext(context.Background(), pdfData, pageNum, dpi)
}

// RenderPDFPageContext is RenderPDFPage with caller-controlled cancellation.
func RenderPDFPageContext(ctx context.Context, pdfData []byte, pageNum int, dpi float64) (reading.BinaryContent, error) {
	if len(pdfData) == 0 {
		return reading.BinaryContent{}, fmt.Errorf("PDF data is empty")
	}
	return renderPDFPageNative(ctx, "-", bytes.NewReader(pdfData), pageNum, dpi)
}

// RenderPDFPageReader renders a PDF supplied on stdin without first buffering
// the whole document in Go.
func RenderPDFPageReader(ctx context.Context, pdfData io.Reader, pageNum int, dpi float64) (reading.BinaryContent, error) {
	if pdfData == nil {
		return reading.BinaryContent{}, fmt.Errorf("PDF reader is required")
	}
	return renderPDFPageNative(ctx, "-", pdfData, pageNum, dpi)
}

// RenderPDFPageFile renders directly from a PDF path, avoiding a caller-side
// copy of the PDF. The native renderer is still required.
func RenderPDFPageFile(ctx context.Context, pdfPath string, pageNum int, dpi float64) (reading.BinaryContent, error) {
	if strings.TrimSpace(pdfPath) == "" {
		return reading.BinaryContent{}, fmt.Errorf("PDF path is required")
	}
	return renderPDFPageNative(ctx, pdfPath, nil, pageNum, dpi)
}

// CheckNativePDFRenderer verifies that ANTFLY_BIN provides the native PDF
// renderer before a batch starts.
func CheckNativePDFRenderer(ctx context.Context) error {
	antflyBin := strings.TrimSpace(os.Getenv("ANTFLY_BIN"))
	if antflyBin == "" {
		return fmt.Errorf("ANTFLY_BIN is required for native PDF rendering")
	}
	stdout := truncatingBuffer{limit: nativeRenderMaxStderrBytes}
	stderr := truncatingBuffer{limit: nativeRenderMaxStderrBytes}
	cmd := exec.CommandContext(ctx, antflyBin, "pdf", "render-page", "--help")
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		detail := strings.TrimSpace(stderr.String())
		if detail == "" {
			detail = strings.TrimSpace(stdout.String())
		}
		if detail != "" {
			return fmt.Errorf("ANTFLY_BIN does not provide native PDF rendering: %w: %s", err, detail)
		}
		return fmt.Errorf("ANTFLY_BIN does not provide native PDF rendering: %w", err)
	}
	return nil
}

func renderPDFPageNative(ctx context.Context, source string, stdin io.Reader, pageNum int, dpi float64) (reading.BinaryContent, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	if pageNum <= 0 {
		return reading.BinaryContent{}, fmt.Errorf("page number must be greater than 0")
	}
	if dpi <= 0 {
		dpi = DefaultRenderDPI
	}

	antflyBin := strings.TrimSpace(os.Getenv("ANTFLY_BIN"))
	if antflyBin == "" {
		return reading.BinaryContent{}, fmt.Errorf("ANTFLY_BIN is required for native PDF rendering")
	}

	args := []string{
		"pdf", "render-page", source,
		"--page", strconv.Itoa(pageNum),
		"--dpi", strconv.FormatFloat(dpi, 'f', -1, 64),
		"--profile", "ocr",
		"--max-pixels", strconv.Itoa(nativeRenderMaxPixels),
		"--max-dimension", strconv.Itoa(nativeRenderMaxDimension),
		"--require-native",
	}
	cmd := exec.CommandContext(ctx, antflyBin, args...)
	if stdin != nil {
		cmd.Stdin = stdin
	}

	stdout := outputLimitBuffer{limit: nativeRenderMaxOutputBytes}
	stderr := truncatingBuffer{limit: nativeRenderMaxStderrBytes}
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if errors.Is(err, errNativeRenderOutputTooLarge) || stdout.tooLarge {
			return reading.BinaryContent{}, fmt.Errorf("render PDF page: %w (%d bytes)", errNativeRenderOutputTooLarge, nativeRenderMaxOutputBytes)
		}
		detail := strings.TrimSpace(stderr.String())
		if detail != "" {
			return reading.BinaryContent{}, fmt.Errorf("native PDF renderer: %w: %s", err, detail)
		}
		return reading.BinaryContent{}, fmt.Errorf("native PDF renderer: %w", err)
	}

	pngBytes := stdout.Bytes()
	geometry, err := png.DecodeConfig(bytes.NewReader(pngBytes))
	if err != nil {
		return reading.BinaryContent{}, fmt.Errorf("native PDF renderer returned invalid PNG output: %w", err)
	}
	if geometry.Width > nativeRenderMaxDimension || geometry.Height > nativeRenderMaxDimension ||
		int64(geometry.Width)*int64(geometry.Height) > nativeRenderMaxPixels {
		return reading.BinaryContent{}, fmt.Errorf("native PDF renderer exceeded image geometry limits")
	}
	reportLine := strings.TrimSpace(stderr.String())
	reportLine = reportLine[strings.LastIndexByte(reportLine, '\n')+1:]
	var report struct {
		Quality string `json:"quality"`
	}
	if err := json.Unmarshal([]byte(reportLine), &report); err != nil {
		return reading.BinaryContent{}, fmt.Errorf("native PDF renderer returned no valid quality report: %w", err)
	}
	if report.Quality != "native" {
		return reading.BinaryContent{}, fmt.Errorf("native PDF renderer reported unacceptable quality %q", report.Quality)
	}
	return reading.BinaryContent{MIMEType: "image/png", Data: pngBytes}, nil
}

type outputLimitBuffer struct {
	bytes.Buffer
	limit    int
	tooLarge bool
}

func (b *outputLimitBuffer) Write(p []byte) (int, error) {
	remaining := b.limit - b.Len()
	if remaining <= 0 {
		b.tooLarge = true
		return 0, errNativeRenderOutputTooLarge
	}
	if len(p) > remaining {
		_, _ = b.Buffer.Write(p[:remaining])
		b.tooLarge = true
		return remaining, errNativeRenderOutputTooLarge
	}
	return b.Buffer.Write(p)
}

type truncatingBuffer struct {
	bytes.Buffer
	limit int
}

func (b *truncatingBuffer) Write(p []byte) (int, error) {
	if remaining := b.limit - b.Len(); remaining > 0 {
		if len(p) < remaining {
			remaining = len(p)
		}
		_, _ = b.Buffer.Write(p[:remaining])
	}
	return len(p), nil
}

// EncodeDataURI encodes binary content as a data URI.
func EncodeDataURI(content reading.BinaryContent) (string, error) {
	mimeType := strings.TrimSpace(content.MIMEType)
	if mimeType == "" {
		return "", fmt.Errorf("mime type is required")
	}
	if len(content.Data) == 0 {
		return "", fmt.Errorf("content data is empty")
	}

	b64 := base64.StdEncoding.EncodeToString(content.Data)
	return "data:" + mimeType + ";base64," + b64, nil
}
