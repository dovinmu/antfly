package main

import (
	"archive/zip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/antflydb/antfly/go/pkg/docsaf/reading"
	antflyreading "github.com/antflydb/antfly/go/pkg/docsaf/reading/antfly"
	inferenceclient "github.com/antflydb/antfly/go/pkg/sdk"
)

const (
	defaultRecoveryOCRModel       = "ggml-org/gemma-4-e2b-it-gguf:gguf:Q4_0"
	defaultRecoveryReaderModel    = "antflydb/Florence-2-base"
	defaultRecoveryOCRMaxTokens   = 2048
	defaultRecoveryVisionTokens   = 256
	defaultRecoveryFallbackTokens = 768
	defaultRecoveryDPI            = 200

	defaultRecoveryOCRPrompt    = "Transcribe all visible text exactly as written. Preserve dates, identifiers, spelling, and abbreviations. Do not infer or reconstruct redacted text. Mark unreadable text [illegible] instead of guessing. Output only the transcription; output nothing if there is no visible text."
	defaultRecoveryVisionPrompt = "Describe the visible layout and appearance of this page in one short sentence. Include visible solid blocks, blank areas, photographs, or drawings. Describe only what is visible; do not transcribe text, infer identities or sensitive attributes, or infer what is concealed. Refer to people generically and keep descriptions non-graphic."
)

var eftaIdentifierRE = regexp.MustCompile(`(?i)^EFTA[0-9]+$`)

func isEFTAIdentifier(text string) bool {
	return eftaIdentifierRE.MatchString(strings.TrimSpace(text))
}

type nativeEnrichConfig struct {
	inputFile           string
	outputFile          string
	inferenceURL        string
	ocrModel            string
	ocrMode             string
	ocrPrompt           string
	ocrMaxTokens        int
	visionModel         string
	visionPrompt        string
	visionMaxTokens     int
	fallbackModel       string
	fallbackMaxTokens   int
	dpi                 float64
	minContentLen       int
	workers             int
	checkpointEvery     int
	dryRun              bool
	reprocess           bool
	onlyEmptyIdentifier bool
	category            string
	dirPath             string
	zipPaths            []string
}

type nativeStageResult struct {
	text       string
	provenance map[string]any
}

type nativeEnrichResult struct {
	candidate     enrichCandidate
	transcription nativeStageResult
	description   nativeStageResult
	sourceSHA256  string
	imageSHA256   string
	renderPage    int
	err           error
}

type nativeEnrichRuntime struct {
	cfg             nativeEnrichConfig
	ocrGenerator    *antflyreading.AntflyGenerateReader
	ocrReader       *antflyreading.AntflyReadReader
	visionGenerator *antflyreading.AntflyGenerateReader
	fallbackReader  *antflyreading.AntflyReadReader
	zipIndex        map[string]*zip.File
}

func runNativeEnrich(args []string) error {
	cfg, err := parseNativeEnrichConfig(args)
	if err != nil {
		return err
	}

	records, err := readJSONFile[map[string]map[string]any](cfg.inputFile)
	if err != nil {
		return fmt.Errorf("read input: %w", err)
	}
	candidates := selectNativeEnrichCandidates(records, cfg)

	fmt.Printf("=== Antfly Native Enrichment ===\n")
	fmt.Printf("Input: %s\n", cfg.inputFile)
	fmt.Printf("Output: %s\n", cfg.outputFile)
	fmt.Printf("Records: %d\n", len(records))
	fmt.Printf("Candidates: %d\n", len(candidates))
	fmt.Printf("OCR: %s (%s)\n", cfg.ocrModel, cfg.ocrMode)
	if cfg.visionModel != "" {
		fmt.Printf("Vision: %s\n", cfg.visionModel)
	}
	if cfg.dryRun {
		return nil
	}
	if len(candidates) == 0 {
		return writeJSONAtomic(cfg.outputFile, records)
	}

	preflightCtx, cancelPreflight := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelPreflight()
	if err := antflyreading.CheckNativePDFRenderer(preflightCtx); err != nil {
		return fmt.Errorf("preflight native PDF renderer: %w", err)
	}

	client, err := inferenceclient.NewInferenceClient(cfg.inferenceURL, &http.Client{Timeout: 15 * time.Minute})
	if err != nil {
		return fmt.Errorf("create inference client: %w", err)
	}
	if err := preflightNativeEnrichModels(preflightCtx, client, cfg); err != nil {
		return err
	}

	zipIndex, zipReaders, err := buildZipIndex(cfg.zipPaths)
	if err != nil {
		return fmt.Errorf("index ZIP sources: %w", err)
	}
	defer func() {
		for _, reader := range zipReaders {
			_ = reader.Close()
		}
	}()

	runtime, err := newNativeEnrichRuntime(client, zipIndex, cfg)
	if err != nil {
		return err
	}
	return runtime.run(records, candidates)
}

func parseNativeEnrichConfig(args []string) (nativeEnrichConfig, error) {
	fs := flag.NewFlagSet("enrich", flag.ExitOnError)
	var cfg nativeEnrichConfig
	var zipPaths StringSliceFlag

	fs.StringVar(&cfg.inputFile, "input", "epstein-docs.json", "Input prepared or checkpoint JSON")
	fs.StringVar(&cfg.outputFile, "output", "", "Output JSON (default: {input-base}-enriched.json)")
	fs.StringVar(&cfg.inferenceURL, "inference-url", defaultInferenceURL(), "Antfly inference URL")
	fs.StringVar(&cfg.ocrModel, "model", defaultRecoveryOCRModel, "Antfly OCR model")
	fs.StringVar(&cfg.ocrMode, "ocr-mode", "generate", "Antfly OCR endpoint: read or generate")
	fs.StringVar(&cfg.ocrPrompt, "prompt", "", "OCR prompt (default depends on --ocr-mode)")
	fs.IntVar(&cfg.ocrMaxTokens, "max-tokens", defaultRecoveryOCRMaxTokens, "Maximum OCR output tokens")
	fs.StringVar(&cfg.visionModel, "vision-model", defaultRecoveryOCRModel, "Antfly visual description model (empty disables descriptions)")
	fs.StringVar(&cfg.visionPrompt, "vision-prompt", defaultRecoveryVisionPrompt, "Visual description prompt")
	fs.IntVar(&cfg.visionMaxTokens, "vision-max-tokens", defaultRecoveryVisionTokens, "Maximum visual description output tokens")
	fs.StringVar(&cfg.fallbackModel, "fallback-reader-model", defaultRecoveryReaderModel, "Antfly reader for rejected OCR or caption generation (empty disables)")
	fs.IntVar(&cfg.fallbackMaxTokens, "fallback-max-tokens", defaultRecoveryFallbackTokens, "Maximum fallback reader output tokens")
	fs.Float64Var(&cfg.dpi, "dpi", defaultRecoveryDPI, "Antfly PDF rendering DPI")
	fs.IntVar(&cfg.minContentLen, "min-content", 50, "Short content threshold")
	fs.IntVar(&cfg.workers, "workers", 1, "Concurrent native inference workers")
	fs.IntVar(&cfg.checkpointEvery, "checkpoint-every", 100, "Atomically checkpoint after this many results")
	fs.BoolVar(&cfg.dryRun, "dry-run", false, "Report candidates without running inference")
	fs.BoolVar(&cfg.reprocess, "reprocess", false, "Reprocess previously enriched source pages")
	fs.BoolVar(&cfg.onlyEmptyIdentifier, "only-empty-or-identifier", false, "Select only originally empty or EFTA-identifier-only records")
	fs.StringVar(&cfg.category, "category", "all", "Candidate category: ocr, vision, quality, or all")
	fs.StringVar(&cfg.dirPath, "dir", "", "Base directory for relative page_pdf_path values")
	fs.Var(&zipPaths, "zip", "ZIP archive containing source PDFs (repeatable)")
	if err := fs.Parse(args); err != nil {
		return nativeEnrichConfig{}, fmt.Errorf("parse flags: %w", err)
	}
	cfg.zipPaths = append([]string(nil), zipPaths...)

	cfg.ocrMode = strings.ToLower(strings.TrimSpace(cfg.ocrMode))
	if cfg.ocrMode != "generate" && cfg.ocrMode != "read" {
		return nativeEnrichConfig{}, fmt.Errorf("invalid --ocr-mode %q (expected read or generate)", cfg.ocrMode)
	}
	cfg.category = strings.ToLower(strings.TrimSpace(cfg.category))
	switch cfg.category {
	case "ocr", "vision", "quality", "all":
	default:
		return nativeEnrichConfig{}, fmt.Errorf("invalid --category %q (expected ocr, vision, quality, or all)", cfg.category)
	}
	if strings.TrimSpace(cfg.ocrModel) == "" {
		return nativeEnrichConfig{}, fmt.Errorf("--model is required")
	}
	if cfg.ocrPrompt == "" {
		if cfg.ocrMode == "read" {
			cfg.ocrPrompt = "<OCR>"
		} else {
			cfg.ocrPrompt = defaultRecoveryOCRPrompt
		}
	}
	if cfg.outputFile == "" {
		base := strings.TrimSuffix(cfg.inputFile, filepath.Ext(cfg.inputFile))
		cfg.outputFile = base + "-enriched.json"
	}
	if cfg.ocrMaxTokens <= 0 || cfg.visionMaxTokens <= 0 || cfg.fallbackMaxTokens <= 0 {
		return nativeEnrichConfig{}, fmt.Errorf("token budgets must be greater than zero")
	}
	if cfg.dpi <= 0 || cfg.minContentLen < 0 || cfg.workers <= 0 || cfg.checkpointEvery <= 0 {
		return nativeEnrichConfig{}, fmt.Errorf("--dpi, --workers, and --checkpoint-every must be positive and --min-content cannot be negative")
	}
	return cfg, nil
}

func selectNativeEnrichCandidates(records map[string]map[string]any, cfg nativeEnrichConfig) []enrichCandidate {
	all := identifyEnrichCandidates(records, cfg.minContentLen, cfg.reprocess)
	selected := make([]enrichCandidate, 0, len(all))
	for _, candidate := range all {
		if cfg.category != "all" && candidate.category != cfg.category {
			continue
		}
		if cfg.onlyEmptyIdentifier {
			original := candidate.content
			if saved, ok := records[candidate.id]["original_content"].(string); ok {
				original = saved
			}
			if strings.TrimSpace(original) != "" && !isEFTAIdentifier(original) {
				continue
			}
		}
		selected = append(selected, candidate)
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].id < selected[j].id })
	return selected
}

func preflightNativeEnrichModels(ctx context.Context, client *inferenceclient.InferenceClient, cfg nativeEnrichConfig) error {
	models, err := client.ListModels(ctx)
	if err != nil {
		return fmt.Errorf("preflight inference models: %w", err)
	}

	generators := map[string]struct{}{}
	readers := map[string]struct{}{}
	if cfg.ocrMode == "generate" {
		generators[cfg.ocrModel] = struct{}{}
	} else {
		readers[cfg.ocrModel] = struct{}{}
	}
	if cfg.visionModel != "" {
		generators[cfg.visionModel] = struct{}{}
	}
	if cfg.fallbackModel != "" {
		readers[cfg.fallbackModel] = struct{}{}
	}
	for model := range generators {
		if _, ok := models.Generators[model]; !ok {
			return fmt.Errorf("preflight inference models: generator %q is not available", model)
		}
	}
	for model := range readers {
		if _, ok := models.Readers[model]; !ok {
			return fmt.Errorf("preflight inference models: reader %q is not available", model)
		}
	}
	return nil
}

func newNativeEnrichRuntime(client *inferenceclient.InferenceClient, zipIndex map[string]*zip.File, cfg nativeEnrichConfig) (*nativeEnrichRuntime, error) {
	zero := float32(0)
	thinking := false
	runtime := &nativeEnrichRuntime{cfg: cfg, zipIndex: zipIndex}

	if cfg.ocrMode == "generate" {
		reader, err := antflyreading.NewAntflyGenerateReader(antflyreading.AntflyConfig{
			Client: client, Models: []string{cfg.ocrModel}, DefaultMaxTokens: cfg.ocrMaxTokens,
			GenerationTemperatureOverride: &zero, EnableThinking: &thinking, AllowEmptyOutput: true,
		})
		if err != nil {
			return nil, fmt.Errorf("create OCR generator: %w", err)
		}
		runtime.ocrGenerator = reader
	} else {
		reader, err := antflyreading.NewAntflyReadReader(antflyreading.AntflyConfig{
			Client: client, Models: []string{cfg.ocrModel}, DefaultMaxTokens: cfg.ocrMaxTokens,
		})
		if err != nil {
			return nil, fmt.Errorf("create OCR reader: %w", err)
		}
		runtime.ocrReader = reader
	}
	if cfg.visionModel != "" {
		reader, err := antflyreading.NewAntflyGenerateReader(antflyreading.AntflyConfig{
			Client: client, Models: []string{cfg.visionModel}, DefaultMaxTokens: cfg.visionMaxTokens,
			GenerationTemperatureOverride: &zero, EnableThinking: &thinking,
		})
		if err != nil {
			return nil, fmt.Errorf("create visual description generator: %w", err)
		}
		runtime.visionGenerator = reader
	}
	if cfg.fallbackModel != "" {
		reader, err := antflyreading.NewAntflyReadReader(antflyreading.AntflyConfig{
			Client: client, Models: []string{cfg.fallbackModel}, DefaultMaxTokens: cfg.fallbackMaxTokens,
		})
		if err != nil {
			return nil, fmt.Errorf("create fallback reader: %w", err)
		}
		runtime.fallbackReader = reader
	}
	return runtime, nil
}

func (r *nativeEnrichRuntime) run(records map[string]map[string]any, candidates []enrichCandidate) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	candidateCh := make(chan enrichCandidate, r.cfg.workers*2)
	resultCh := make(chan nativeEnrichResult, r.cfg.workers*2)
	go func() {
		defer close(candidateCh)
		for _, candidate := range candidates {
			select {
			case candidateCh <- candidate:
			case <-ctx.Done():
				return
			}
		}
	}()

	var workers sync.WaitGroup
	workers.Add(r.cfg.workers)
	for range r.cfg.workers {
		go func() {
			defer workers.Done()
			for candidate := range candidateCh {
				result := r.processCandidate(ctx, candidate)
				select {
				case resultCh <- result:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	go func() {
		workers.Wait()
		close(resultCh)
	}()

	var completed, enriched, failed int
	var checkpointErr error
	for result := range resultCh {
		completed++
		if result.err != nil {
			failed++
			markNativeEnrichFailure(records[result.candidate.id], result.err)
			log.Printf("Error enriching %s: %v", result.candidate.id, result.err)
		} else {
			enriched++
			applyNativeEnrichResult(records[result.candidate.id], result, r.cfg)
		}

		if checkpointErr == nil && completed%r.cfg.checkpointEvery == 0 {
			if err := writeJSONAtomic(r.cfg.outputFile, records); err != nil {
				checkpointErr = fmt.Errorf("checkpoint after %d results: %w", completed, err)
				cancel()
			}
		}
		if completed%100 == 0 || completed == len(candidates) {
			fmt.Printf("Processed %d/%d (enriched=%d failed=%d)\n", completed, len(candidates), enriched, failed)
		}
	}
	if checkpointErr != nil {
		return checkpointErr
	}
	if err := writeJSONAtomic(r.cfg.outputFile, records); err != nil {
		return fmt.Errorf("write final checkpoint: %w", err)
	}
	fmt.Printf("Wrote %s (enriched=%d failed=%d)\n", r.cfg.outputFile, enriched, failed)
	if failed > 0 {
		return fmt.Errorf("enrichment failed for %d record(s); successful records were checkpointed at %s and failed records remain resumable", failed, r.cfg.outputFile)
	}
	return nil
}

func (r *nativeEnrichRuntime) processCandidate(ctx context.Context, candidate enrichCandidate) nativeEnrichResult {
	result := nativeEnrichResult{candidate: candidate}
	page, sourceHash, imageHash, renderPage, err := r.renderCandidate(ctx, candidate)
	if err != nil {
		result.err = err
		return result
	}
	result.sourceSHA256 = sourceHash
	result.imageSHA256 = imageHash
	result.renderPage = renderPage

	dataURI, err := antflyreading.EncodeDataURI(page)
	if err != nil {
		result.err = fmt.Errorf("encode rendered page: %w", err)
		return result
	}
	result.transcription, err = r.transcribe(ctx, dataURI)
	if err != nil {
		result.err = fmt.Errorf("native OCR: %w", err)
		return result
	}
	if r.visionGenerator != nil {
		result.description, err = r.describe(ctx, dataURI)
		if err != nil {
			result.err = fmt.Errorf("native description: %w", err)
			return result
		}
	}
	return result
}

func (r *nativeEnrichRuntime) renderCandidate(ctx context.Context, candidate enrichCandidate) (reading.BinaryContent, string, string, int, error) {
	var sourceErrors []error
	if candidate.pdfPath != "" {
		path := candidate.pdfPath
		if r.cfg.dirPath != "" && !filepath.IsAbs(path) {
			path = filepath.Join(r.cfg.dirPath, path)
		}
		sourceHash, err := hashFile(path)
		if err == nil {
			page, renderErr := antflyreading.RenderPDFPageFile(ctx, path, 1, r.cfg.dpi)
			if renderErr == nil {
				return page, sourceHash, sha256Hex(page.Data), 1, nil
			}
			sourceErrors = append(sourceErrors, fmt.Errorf("render %s: %w", path, renderErr))
		} else {
			sourceErrors = append(sourceErrors, fmt.Errorf("read %s: %w", path, err))
		}
	}
	if r.zipIndex != nil && candidate.sourceFile != "" && candidate.pageNum > 0 {
		entry, err := lookupZipPDF(r.zipIndex, candidate.sourceFile)
		if err == nil {
			source, openErr := entry.Open()
			if openErr != nil {
				sourceErrors = append(sourceErrors, fmt.Errorf("open ZIP source: %w", openErr))
			} else {
				hasher := sha256.New()
				page, renderErr := antflyreading.RenderPDFPageReader(ctx, io.TeeReader(source, hasher), candidate.pageNum, r.cfg.dpi)
				if renderErr == nil {
					_, hashErr := io.Copy(hasher, source)
					closeErr := source.Close()
					if hashErr == nil && closeErr == nil {
						return page, hex.EncodeToString(hasher.Sum(nil)), sha256Hex(page.Data), candidate.pageNum, nil
					}
					sourceErrors = append(sourceErrors, fmt.Errorf("hash ZIP source: %w", errors.Join(hashErr, closeErr)))
				} else {
					_ = source.Close()
					sourceErrors = append(sourceErrors, fmt.Errorf("render ZIP page: %w", renderErr))
				}
			}
		} else {
			sourceErrors = append(sourceErrors, err)
		}
	}
	if len(sourceErrors) == 0 {
		return reading.BinaryContent{}, "", "", 0, fmt.Errorf("no page source available")
	}
	return reading.BinaryContent{}, "", "", 0, errors.Join(sourceErrors...)
}

func lookupZipPDF(index map[string]*zip.File, sourceFile string) (*zip.File, error) {
	normalized := strings.TrimPrefix(filepath.ToSlash(filepath.Clean(sourceFile)), "./")
	if entry := index[normalized]; entry != nil {
		return entry, nil
	}
	if entry := index[filepath.Base(normalized)]; entry != nil {
		return entry, nil
	}
	return nil, fmt.Errorf("source file %q not found in ZIP", sourceFile)
}

func (r *nativeEnrichRuntime) transcribe(ctx context.Context, dataURI string) (nativeStageResult, error) {
	prompt := r.cfg.ocrPrompt
	maxTokens := r.cfg.ocrMaxTokens
	var result antflyreading.Result
	var err error
	if r.ocrGenerator != nil {
		result, err = r.ocrGenerator.GenerateDataURIDetailed(ctx, dataURI, &reading.ReadOptions{Prompt: prompt, MaxTokens: maxTokens})
	} else {
		result, err = r.ocrReader.ReadDataURIDetailed(ctx, dataURI, &reading.ReadOptions{Prompt: prompt, MaxTokens: maxTokens})
	}
	if err == nil {
		return nativeStageResult{
			text:       result.Text,
			provenance: map[string]any{"endpoint": r.cfg.ocrMode, "model": result.Model, "prompt": prompt, "max_tokens": maxTokens},
		}, nil
	}
	return r.fallbackRead(ctx, dataURI, "<OCR>", true, map[string]any{
		"endpoint": r.cfg.ocrMode, "requested_model": r.cfg.ocrModel, "prompt": prompt, "max_tokens": maxTokens, "error": err.Error(),
	})
}

func (r *nativeEnrichRuntime) describe(ctx context.Context, dataURI string) (nativeStageResult, error) {
	prompt := r.cfg.visionPrompt
	maxTokens := r.cfg.visionMaxTokens
	result, err := r.visionGenerator.GenerateDataURIDetailed(ctx, dataURI, &reading.ReadOptions{Prompt: prompt, MaxTokens: maxTokens})
	if err == nil && strings.TrimSpace(result.Text) != "" {
		return nativeStageResult{
			text:       result.Text,
			provenance: map[string]any{"endpoint": "generate", "model": result.Model, "prompt": prompt, "max_tokens": maxTokens},
		}, nil
	}
	if err == nil {
		err = fmt.Errorf("generate endpoint returned an empty visual description")
	}
	return r.fallbackRead(ctx, dataURI, "<CAPTION>", false, map[string]any{
		"endpoint": "generate", "requested_model": r.cfg.visionModel, "prompt": prompt, "max_tokens": maxTokens, "error": err.Error(),
	})
}

func (r *nativeEnrichRuntime) fallbackRead(ctx context.Context, dataURI, prompt string, allowEmpty bool, fallbackFrom map[string]any) (nativeStageResult, error) {
	if r.fallbackReader == nil {
		return nativeStageResult{}, fmt.Errorf("%v; native reader fallback is disabled", fallbackFrom["error"])
	}
	result, err := r.fallbackReader.ReadDataURIDetailed(ctx, dataURI, &reading.ReadOptions{
		Prompt: prompt, MaxTokens: r.cfg.fallbackMaxTokens,
	})
	if err != nil {
		return nativeStageResult{}, fmt.Errorf("%v; native reader fallback: %w", fallbackFrom["error"], err)
	}
	if !allowEmpty && strings.TrimSpace(result.Text) == "" {
		return nativeStageResult{}, fmt.Errorf("%v; native reader fallback returned an empty description", fallbackFrom["error"])
	}
	return nativeStageResult{
		text: result.Text,
		provenance: map[string]any{
			"endpoint": "read", "model": result.Model, "prompt": prompt,
			"max_tokens": r.cfg.fallbackMaxTokens, "fallback_from": fallbackFrom,
		},
	}, nil
}

func applyNativeEnrichResult(record map[string]any, result nativeEnrichResult, cfg nativeEnrichConfig) {
	metadata, _ := record["metadata"].(map[string]any)
	if metadata == nil {
		metadata = map[string]any{}
		record["metadata"] = metadata
	}
	original := result.candidate.content
	if saved, ok := record["original_content"].(string); ok {
		original = saved
	} else {
		record["original_content"] = original
	}
	if _, exists := metadata["original_extraction_method"]; !exists {
		if method, ok := metadata["extraction_method"].(string); ok && method != "" {
			metadata["original_extraction_method"] = method
		}
	}

	record["transcription"] = result.transcription.text
	if cfg.visionModel != "" {
		record["visual_description"] = result.description.text
	} else {
		delete(record, "visual_description")
	}
	record["content"] = composeRecoveredContent(result.transcription.text, result.description.text)

	recovery := map[string]any{
		"provider":             "antfly",
		"renderer":             "antfly_pdf",
		"render_quality":       "native",
		"requested_render_dpi": cfg.dpi,
		"source_sha256":        result.sourceSHA256,
		"image_sha256":         result.imageSHA256,
		"render_page":          result.renderPage,
		"inference_url":        cfg.inferenceURL,
		"reasons":              result.candidate.reasons,
		"review_status":        "machine_generated_not_reviewed",
		"generation": map[string]any{
			"temperature": 0, "enable_thinking": false,
		},
		"ocr": result.transcription.provenance,
	}
	if cfg.visionModel != "" {
		recovery["vision"] = result.description.provenance
	}
	metadata["recovery"] = recovery
	metadata["extraction_method"] = "antfly_native"
	metadata["enriched"] = true
	delete(metadata, "enrich_error")
}

func composeRecoveredContent(transcription, description string) string {
	var sections []string
	if strings.TrimSpace(transcription) != "" {
		sections = append(sections, "OCR transcription (Antfly-generated):\n"+transcription)
	}
	if strings.TrimSpace(description) != "" {
		sections = append(sections, "Image description (Antfly-generated, not transcription):\n"+description)
	}
	return strings.Join(sections, "\n\n")
}

func markNativeEnrichFailure(record map[string]any, err error) {
	metadata, _ := record["metadata"].(map[string]any)
	if metadata == nil {
		metadata = map[string]any{}
		record["metadata"] = metadata
	}
	metadata["enrich_error"] = err.Error()
	if enriched, _ := metadata["enriched"].(bool); !enriched {
		metadata["enriched"] = false
	}
}

func hashFile(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	h := sha256.New()
	if _, err := io.Copy(h, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func writeJSONAtomic(path string, records map[string]map[string]any) error {
	dir := filepath.Dir(path)
	temp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	if err := temp.Close(); err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	defer os.Remove(tempPath)

	mode := os.FileMode(0o644)
	if info, err := os.Stat(path); err == nil {
		mode = info.Mode().Perm()
	}
	if err := os.Chmod(tempPath, mode); err != nil {
		return err
	}
	if err := writeJSONSorted(tempPath, records); err != nil {
		return err
	}
	file, err := os.Open(tempPath)
	if err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempPath, path); err != nil {
		return err
	}
	if directory, err := os.Open(dir); err == nil {
		_ = directory.Sync()
		_ = directory.Close()
	}
	return nil
}
