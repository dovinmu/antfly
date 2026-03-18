package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"slices"
	"strings"
	"time"

	antfly "github.com/antflydb/antfly/pkg/client"
	"github.com/antflydb/antfly/pkg/docsaf"
	termiteclient "github.com/antflydb/termite/pkg/client"
	"github.com/cespare/xxhash/v2"
)

// StringSliceFlag allows repeated flags to build a slice
type StringSliceFlag []string

func (s *StringSliceFlag) String() string {
	return strings.Join(*s, ", ")
}

func (s *StringSliceFlag) Set(value string) error {
	*s = append(*s, value)
	return nil
}

// nerFlags holds the parsed NER-related command-line flags.
type nerFlags struct {
	termiteURL string
	model      string
	threshold  float64
	batchSize  int
	labels     StringSliceFlag
}

// registerNERFlags registers NER-related flags on the given FlagSet.
func registerNERFlags(fs *flag.FlagSet) *nerFlags {
	nf := &nerFlags{}
	fs.StringVar(&nf.termiteURL, "termite-url", "http://localhost:8088", "Termite API URL for NER")
	fs.StringVar(&nf.model, "ner-model", "", "Termite recognizer model for entity extraction (e.g., fastino/gliner2-base-v1)")
	fs.Float64Var(&nf.threshold, "ner-threshold", 0.5, "Minimum confidence score for extracted entities")
	fs.IntVar(&nf.batchSize, "ner-batch-size", 32, "Number of texts per NER batch")
	fs.Var(&nf.labels, "ner-label", "Entity label for zero-shot NER (can be repeated, e.g., --ner-label technology --ner-label concept)")
	return nf
}

// enabled returns true if a NER model was specified.
func (nf *nerFlags) enabled() bool {
	return nf.model != ""
}

// printConfig prints the NER configuration to stdout.
func (nf *nerFlags) printConfig() {
	if !nf.enabled() {
		return
	}
	fmt.Printf("NER model: %s\n", nf.model)
	if len(nf.labels) > 0 {
		fmt.Printf("NER labels: %v\n", nf.labels)
	}
	fmt.Printf("NER threshold: %.2f\n", nf.threshold)
}

// run performs entity extraction if NER is enabled, returning nil result if disabled.
func (nf *nerFlags) run(ctx context.Context, sections []docsaf.DocumentSection) (*entityResult, error) {
	if !nf.enabled() {
		return nil, nil
	}

	if len(nf.labels) == 0 {
		return nil, fmt.Errorf("at least one --ner-label is required when using --ner-model (GLiNER2 requires labels for zero-shot NER)")
	}

	tc, err := termiteclient.NewTermiteClient(nf.termiteURL, http.DefaultClient)
	if err != nil {
		return nil, fmt.Errorf("failed to create termite client: %w", err)
	}

	return extractEntities(ctx, tc, nf.model, nf.labels, float32(nf.threshold), sections, nf.batchSize)
}

// entityResult holds the output of NER entity extraction.
type entityResult struct {
	// entityRecords maps entity key to entity document
	entityRecords map[string]map[string]any
	// sectionEntityKeys maps section index to entity keys mentioned in that section
	sectionEntityKeys map[int][]string
}

// normalizeEntityKey creates a stable, sortable key from an entity label and text.
// Example: normalizeEntityKey("technology", "Raft Consensus") returns "entity:technology:raft-consensus"
// Non-ASCII characters are preserved via hex encoding to avoid silent collisions.
func normalizeEntityKey(label, text string) string {
	normalized := strings.ToLower(strings.TrimSpace(text))
	normalized = strings.ReplaceAll(normalized, " ", "-")
	var b strings.Builder
	for _, r := range normalized {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			b.WriteRune(r)
		} else if r > 127 {
			// Hex-encode non-ASCII to avoid silent collisions
			fmt.Fprintf(&b, "%x", r)
		}
	}
	key := b.String()
	if key == "" {
		// Fallback for names that normalize to empty (e.g., all punctuation)
		key = fmt.Sprintf("%x", xxhash.Sum64String(text))
	}
	return fmt.Sprintf("entity:%s:%s", strings.ToLower(label), key)
}

// extractEntities calls termite's NER API to extract named entities from document sections.
func extractEntities(
	ctx context.Context,
	tc *termiteclient.TermiteClient,
	model string,
	labels []string,
	threshold float32,
	sections []docsaf.DocumentSection,
	batchSize int,
) (*entityResult, error) {
	result := &entityResult{
		entityRecords:     make(map[string]map[string]any),
		sectionEntityKeys: make(map[int][]string),
	}

	totalBatches := (len(sections) + batchSize - 1) / batchSize
	fmt.Printf("Extracting entities with %s (%d labels, threshold %.2f)\n", model, len(labels), threshold)
	fmt.Printf("Processing %d sections in %d NER batches of %d\n\n", len(sections), totalBatches, batchSize)

	for start := 0; start < len(sections); start += batchSize {
		end := min(start+batchSize, len(sections))
		batchNum := start/batchSize + 1

		// Build texts: title + content for better extraction context
		texts := make([]string, end-start)
		for i, s := range sections[start:end] {
			texts[i] = s.Title + "\n\n" + s.Content
		}

		fmt.Printf("[NER Batch %d/%d] Processing sections %d-%d\n", batchNum, totalBatches, start+1, end)

		resp, err := tc.Recognize(ctx, model, texts, labels)
		if err != nil {
			return nil, fmt.Errorf("NER batch %d (sections %d-%d): %w", batchNum, start+1, end, err)
		}

		batchEntities := 0
		for i, entities := range resp.Entities {
			sectionIdx := start + i
			var keys []string
			seen := make(map[string]bool)

			for _, entity := range entities {
				if entity.Score < threshold {
					continue
				}

				key := normalizeEntityKey(entity.Label, entity.Text)
				if seen[key] {
					continue
				}
				seen[key] = true
				keys = append(keys, key)
				batchEntities++

				if existing, ok := result.entityRecords[key]; ok {
					if cnt, ok := existing["mention_count"].(int); ok {
						existing["mention_count"] = cnt + 1
					}
				} else {
					result.entityRecords[key] = map[string]any{
						"id":            key,
						"name":          entity.Text,
						"label":         entity.Label,
						"_type":         "entity",
						"mention_count": 1,
					}
				}
			}

			if len(keys) > 0 {
				result.sectionEntityKeys[sectionIdx] = keys
			}
		}
		fmt.Printf("  Found %d entity mentions across %d sections\n", batchEntities, end-start)
	}

	fmt.Printf("\nEntity extraction complete: %d unique entities across %d sections\n",
		len(result.entityRecords), len(result.sectionEntityKeys))

	// Print entity type distribution
	labelCounts := make(map[string]int)
	for _, doc := range result.entityRecords {
		labelCounts[doc["label"].(string)]++
	}
	fmt.Printf("Entity types:\n")
	for label, count := range labelCounts {
		fmt.Printf("  - %s: %d\n", label, count)
	}
	fmt.Printf("\n")

	return result, nil
}

// buildRecords converts sections to a records map, optionally enriching with entity data.
func buildRecords(sections []docsaf.DocumentSection, nerResult *entityResult) map[string]any {
	records := make(map[string]any)
	for i, section := range sections {
		doc := section.ToDocument()
		if nerResult != nil {
			if keys, ok := nerResult.sectionEntityKeys[i]; ok {
				doc["entities"] = keys
			}
		}
		records[section.ID] = doc
	}

	if nerResult != nil {
		for key, entityDoc := range nerResult.entityRecords {
			records[key] = entityDoc
		}
	}

	return records
}

// hasEntityRecords checks if any records in the map are entity documents.
func hasEntityRecords(records map[string]any) bool {
	for _, v := range records {
		if doc, ok := v.(map[string]any); ok {
			if docType, ok := doc["_type"]; ok && docType == "entity" {
				return true
			}
		}
	}
	return false
}

// ANCHOR: prepare_cmd
func prepareCmd(args []string) error {
	fs := flag.NewFlagSet("prepare", flag.ExitOnError)
	dirPath := fs.String("dir", "", "Path to directory containing documentation files (required)")
	outputFile := fs.String("output", "docs.json", "Output JSON file path")
	baseURL := fs.String("base-url", "", "Base URL for generating document links (optional)")

	ner := registerNERFlags(fs)

	var includePatterns StringSliceFlag
	var excludePatterns StringSliceFlag
	fs.Var(&includePatterns, "include", "Include pattern (can be repeated, supports ** wildcards)")
	fs.Var(&excludePatterns, "exclude", "Exclude pattern (can be repeated, supports ** wildcards)")

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("failed to parse flags: %w", err)
	}

	if *dirPath == "" {
		return fmt.Errorf("--dir flag is required")
	}

	// Verify path exists and is a directory
	fileInfo, err := os.Stat(*dirPath)
	if err != nil {
		return fmt.Errorf("failed to access path: %w", err)
	}

	if !fileInfo.IsDir() {
		return fmt.Errorf("--dir must be a directory")
	}

	fmt.Printf("=== docsaf prepare - Process Documentation Files ===\n")
	fmt.Printf("Directory: %s\n", *dirPath)
	fmt.Printf("Output: %s\n", *outputFile)
	if len(includePatterns) > 0 {
		fmt.Printf("Include patterns: %v\n", includePatterns)
	}
	if len(excludePatterns) > 0 {
		fmt.Printf("Exclude patterns: %v\n", excludePatterns)
	}
	ner.printConfig()
	fmt.Printf("\n")

	// Create filesystem source and processor using library
	source := docsaf.NewFilesystemSource(docsaf.FilesystemSourceConfig{
		BaseDir:         *dirPath,
		BaseURL:         *baseURL,
		IncludePatterns: includePatterns,
		ExcludePatterns: excludePatterns,
	})
	processor := docsaf.NewProcessor(source, docsaf.DefaultRegistry())

	// Process all files in the directory
	fmt.Printf("Processing documentation files (chunking by markdown headings)...\n")
	sections, err := processor.Process(context.Background())
	if err != nil {
		return fmt.Errorf("failed to process directory: %w", err)
	}

	fmt.Printf("Found %d documents\n\n", len(sections))

	if len(sections) == 0 {
		return fmt.Errorf("no supported files found in directory")
	}

	// Count sections by type
	typeCounts := make(map[string]int)
	for _, section := range sections {
		typeCounts[section.Type]++
	}

	fmt.Printf("Document types found:\n")
	for docType, count := range typeCounts {
		fmt.Printf("  - %s: %d\n", docType, count)
	}
	fmt.Printf("\n")

	// Show sample of documents
	fmt.Printf("Sample documents:\n")
	for i, section := range sections {
		if i >= 10 {
			fmt.Printf("  ... and %d more\n", len(sections)-i)
			break
		}
		fmt.Printf("  [%d] %s (%s) - %s\n",
			i+1, section.Title, section.Type, section.FilePath)
	}
	fmt.Printf("\n")

	// Extract entities if NER model specified
	nerResult, err := ner.run(context.Background(), sections)
	if err != nil {
		return fmt.Errorf("entity extraction failed: %w", err)
	}

	// Convert sections to records map (enriched with entities if NER was used)
	records := buildRecords(sections, nerResult)

	// Write to JSON file
	fmt.Printf("Writing %d records to %s...\n", len(records), *outputFile)
	jsonData, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}

	err = os.WriteFile(*outputFile, jsonData, 0644)
	if err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	fmt.Printf("Prepared data written to %s\n", *outputFile)
	return nil
}

// ANCHOR_END: prepare_cmd

// ANCHOR: load_cmd
func loadCmd(args []string) error {
	fs := flag.NewFlagSet("load", flag.ExitOnError)
	antflyURL := fs.String("url", "http://localhost:8080/api/v1", "Antfly API URL")
	tableName := fs.String("table", "docs", "Table name to merge into")
	inputFile := fs.String("input", "docs.json", "Input JSON file path")
	dryRun := fs.Bool("dry-run", false, "Preview changes without applying them")
	createTable := fs.Bool("create-table", false, "Create table if it doesn't exist")
	numShards := fs.Int("num-shards", 1, "Number of shards for new table")
	batchSize := fs.Int("batch-size", 25, "Linear merge batch size")
	embeddingModel := fs.String("embedding-model", "embeddinggemma", "Embedding model to use (e.g., embeddinggemma)")
	chunkerModel := fs.String("chunker-model", "fixed-bert-tokenizer", "Chunker model: fixed-bert-tokenizer, fixed-bpe-tokenizer, or any ONNX model directory name")
	targetTokens := fs.Int("target-tokens", 512, "Target tokens for chunking")
	overlapTokens := fs.Int("overlap-tokens", 50, "Overlap tokens for chunking")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("failed to parse flags: %w", err)
	}

	ctx := context.Background()

	// Create Antfly client
	client, err := antfly.NewAntflyClient(*antflyURL, http.DefaultClient)
	if err != nil {
		return fmt.Errorf("failed to create Antfly client: %w", err)
	}

	fmt.Printf("=== docsaf load - Load Data to Antfly ===\n")
	fmt.Printf("Antfly URL: %s\n", *antflyURL)
	fmt.Printf("Table: %s\n", *tableName)
	fmt.Printf("Input: %s\n", *inputFile)
	fmt.Printf("Dry run: %v\n\n", *dryRun)

	// Read JSON file first (needed to detect entity records for graph index)
	fmt.Printf("Reading records from %s...\n", *inputFile)
	jsonData, err := os.ReadFile(*inputFile)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	var records map[string]any
	err = json.Unmarshal(jsonData, &records)
	if err != nil {
		return fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	fmt.Printf("Loaded %d records\n\n", len(records))

	// Create table if requested
	if *createTable {
		fmt.Printf("Creating table '%s' with %d shards...\n", *tableName, *numShards)

		// Create embedding index configuration
		embeddingIndex, err := createEmbeddingIndex(*embeddingModel, *chunkerModel, *targetTokens, *overlapTokens)
		if err != nil {
			return fmt.Errorf("failed to create embedding index config: %w", err)
		}

		indexes := map[string]antfly.IndexConfig{
			"embeddings": *embeddingIndex,
		}

		// Auto-detect entity records and add graph index
		if hasEntityRecords(records) {
			graphIndex, err := createGraphIndex()
			if err != nil {
				return fmt.Errorf("failed to create graph index config: %w", err)
			}
			indexes["knowledge"] = *graphIndex
			fmt.Printf("Detected entity records, adding knowledge graph index\n")
		}

		if err := createTableWithIndexes(ctx, client, *tableName, *numShards, indexes); err != nil {
			return fmt.Errorf("error creating table: %w", err)
		}
	}

	// Perform batched linear merge
	finalCursor, err := performBatchedLinearMerge(ctx, client, *tableName, records, *batchSize, *dryRun)
	if err != nil {
		return fmt.Errorf("batched linear merge failed: %w", err)
	}

	// Final cleanup: delete any remaining documents beyond the last cursor
	if finalCursor != "" && !*dryRun {
		fmt.Printf("\nPerforming final cleanup to remove orphaned documents...\n")
		cleanupResult, err := client.LinearMerge(ctx, *tableName, antfly.LinearMergeRequest{
			Records:      map[string]any{}, // Empty records
			LastMergedId: finalCursor,       // Start from last cursor
			DryRun:       false,
			SyncLevel:    antfly.SyncLevelAknn,
		})
		if err != nil {
			return fmt.Errorf("final cleanup failed: %w", err)
		}
		fmt.Printf("Final cleanup completed in %s\n", cleanupResult.Took)
		fmt.Printf("  Deleted: %d orphaned documents\n", cleanupResult.Deleted)
	}

	fmt.Printf("\nLoad completed successfully\n")
	return nil
}

// ANCHOR_END: load_cmd

// ANCHOR: sync_cmd
func syncCmd(args []string) error {
	fs := flag.NewFlagSet("sync", flag.ExitOnError)
	antflyURL := fs.String("url", "http://localhost:8080/api/v1", "Antfly API URL")
	tableName := fs.String("table", "docs", "Table name to merge into")
	dirPath := fs.String("dir", "", "Path to directory containing documentation files (required)")
	baseURL := fs.String("base-url", "", "Base URL for generating document links (optional)")
	dryRun := fs.Bool("dry-run", false, "Preview changes without applying them")
	createTable := fs.Bool("create-table", false, "Create table if it doesn't exist")
	numShards := fs.Int("num-shards", 1, "Number of shards for new table")
	batchSize := fs.Int("batch-size", 25, "Linear merge batch size")
	embeddingModel := fs.String("embedding-model", "embeddinggemma", "Embedding model to use (e.g., embeddinggemma)")
	chunkerModel := fs.String("chunker-model", "fixed-bert-tokenizer", "Chunker model: fixed-bert-tokenizer, fixed-bpe-tokenizer, or any ONNX model directory name")
	targetTokens := fs.Int("target-tokens", 512, "Target tokens for chunking")
	overlapTokens := fs.Int("overlap-tokens", 50, "Overlap tokens for chunking")

	ner := registerNERFlags(fs)

	var includePatterns StringSliceFlag
	var excludePatterns StringSliceFlag
	fs.Var(&includePatterns, "include", "Include pattern (can be repeated, supports ** wildcards)")
	fs.Var(&excludePatterns, "exclude", "Exclude pattern (can be repeated, supports ** wildcards)")

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("failed to parse flags: %w", err)
	}

	if *dirPath == "" {
		return fmt.Errorf("--dir flag is required")
	}

	// Verify path exists and is a directory before any remote operations
	fileInfo, err := os.Stat(*dirPath)
	if err != nil {
		return fmt.Errorf("failed to access path: %w", err)
	}
	if !fileInfo.IsDir() {
		return fmt.Errorf("--dir must be a directory")
	}

	ctx := context.Background()

	// Create Antfly client
	client, err := antfly.NewAntflyClient(*antflyURL, http.DefaultClient)
	if err != nil {
		return fmt.Errorf("failed to create Antfly client: %w", err)
	}

	fmt.Printf("=== docsaf sync - Full Pipeline ===\n")
	fmt.Printf("Antfly URL: %s\n", *antflyURL)
	fmt.Printf("Table: %s\n", *tableName)
	fmt.Printf("Directory: %s\n", *dirPath)
	fmt.Printf("Dry run: %v\n", *dryRun)
	if len(includePatterns) > 0 {
		fmt.Printf("Include patterns: %v\n", includePatterns)
	}
	if len(excludePatterns) > 0 {
		fmt.Printf("Exclude patterns: %v\n", excludePatterns)
	}
	ner.printConfig()
	fmt.Printf("\n")

	// Create table if requested
	if *createTable {
		fmt.Printf("Creating table '%s' with %d shards...\n", *tableName, *numShards)

		// Create embedding index configuration
		embeddingIndex, err := createEmbeddingIndex(*embeddingModel, *chunkerModel, *targetTokens, *overlapTokens)
		if err != nil {
			return fmt.Errorf("failed to create embedding index config: %w", err)
		}

		indexes := map[string]antfly.IndexConfig{
			"embeddings": *embeddingIndex,
		}

		// Add graph index when NER is enabled
		if ner.enabled() {
			graphIndex, err := createGraphIndex()
			if err != nil {
				return fmt.Errorf("failed to create graph index config: %w", err)
			}
			indexes["knowledge"] = *graphIndex
		}

		if err := createTableWithIndexes(ctx, client, *tableName, *numShards, indexes); err != nil {
			return fmt.Errorf("error creating table: %w", err)
		}
	}

	// Create filesystem source and processor using library
	source := docsaf.NewFilesystemSource(docsaf.FilesystemSourceConfig{
		BaseDir:         *dirPath,
		BaseURL:         *baseURL,
		IncludePatterns: includePatterns,
		ExcludePatterns: excludePatterns,
	})
	processor := docsaf.NewProcessor(source, docsaf.DefaultRegistry())

	// Process all files in the directory
	fmt.Printf("Processing documentation files (chunking by markdown headings)...\n")
	sections, err := processor.Process(context.Background())
	if err != nil {
		return fmt.Errorf("failed to process directory: %w", err)
	}

	fmt.Printf("Found %d documents\n\n", len(sections))

	if len(sections) == 0 {
		return fmt.Errorf("no supported files found in directory")
	}

	// Count sections by type
	typeCounts := make(map[string]int)
	for _, section := range sections {
		typeCounts[section.Type]++
	}

	fmt.Printf("Document types found:\n")
	for docType, count := range typeCounts {
		fmt.Printf("  - %s: %d\n", docType, count)
	}
	fmt.Printf("\n")

	// Show sample of documents
	fmt.Printf("Sample documents:\n")
	for i, section := range sections {
		if i >= 10 {
			fmt.Printf("  ... and %d more\n", len(sections)-i)
			break
		}
		fmt.Printf("  [%d] %s (%s) - %s\n",
			i+1, section.Title, section.Type, section.FilePath)
	}
	fmt.Printf("\n")

	// Extract entities if NER model specified
	nerResult, err := ner.run(ctx, sections)
	if err != nil {
		return fmt.Errorf("entity extraction failed: %w", err)
	}

	// Convert sections to records map (enriched with entities if NER was used)
	records := buildRecords(sections, nerResult)

	// Perform batched linear merge
	finalCursor, err := performBatchedLinearMerge(ctx, client, *tableName, records, *batchSize, *dryRun)
	if err != nil {
		return fmt.Errorf("batched linear merge failed: %w", err)
	}

	// Final cleanup: delete any remaining documents beyond the last cursor
	if finalCursor != "" && !*dryRun {
		fmt.Printf("\nPerforming final cleanup to remove orphaned documents...\n")
		cleanupResult, err := client.LinearMerge(ctx, *tableName, antfly.LinearMergeRequest{
			Records:      map[string]any{}, // Empty records
			LastMergedId: finalCursor,       // Start from last cursor
			DryRun:       false,
			SyncLevel:    antfly.SyncLevelAknn,
		})
		if err != nil {
			return fmt.Errorf("final cleanup failed: %w", err)
		}
		fmt.Printf("Final cleanup completed in %s\n", cleanupResult.Took)
		fmt.Printf("  Deleted: %d orphaned documents\n", cleanupResult.Deleted)
	}

	fmt.Printf("\nSync completed successfully\n")
	return nil
}

// ANCHOR_END: sync_cmd

// ANCHOR: create_embedding_index
// createEmbeddingIndex creates an embedding index configuration with chunking
func createEmbeddingIndex(embeddingModel, chunkerModel string, targetTokens, overlapTokens int) (*antfly.IndexConfig, error) {
	embeddingIndexConfig := antfly.IndexConfig{
		Name: "embeddings",
		Type: antfly.IndexTypeEmbeddings,
	}

	// Configure embedder
	embedder, err := antfly.NewEmbedderConfig(antfly.OllamaEmbedderConfig{
		Model: embeddingModel,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to configure embedder: %w", err)
	}

	// Configure chunker via Termite
	// Model can be "fixed-bert-tokenizer", "fixed-bpe-tokenizer", or any ONNX model directory name
	chunker := antfly.ChunkerConfig{}
	err = chunker.FromTermiteChunkerConfig(antfly.TermiteChunkerConfig{
		Model: chunkerModel,
		Text: antfly.TextChunkOptions{
			TargetTokens:  targetTokens,
			OverlapTokens: overlapTokens,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to configure chunker: %w", err)
	}

	// Configure embedding index with chunking
	// Note: Dimension is calculated automatically based on the embedding model
	err = embeddingIndexConfig.FromEmbeddingsIndexConfig(antfly.EmbeddingsIndexConfig{
		Field:    "content",
		Embedder: *embedder,
		Chunker:  chunker,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to configure embedding index: %w", err)
	}

	return &embeddingIndexConfig, nil
}

// ANCHOR_END: create_embedding_index

// createGraphIndex creates a graph index with a mentions_entity edge type.
// The edge type uses field-based extraction: it reads the "entities" array field
// from each document and automatically creates edges to the referenced entity nodes.
func createGraphIndex() (*antfly.IndexConfig, error) {
	graphIndexConfig := antfly.IndexConfig{
		Name: "knowledge",
		Type: antfly.IndexTypeGraph,
	}

	err := graphIndexConfig.FromGraphIndexConfig(antfly.GraphIndexConfig{
		EdgeTypes: []antfly.EdgeTypeConfig{
			{
				Name:  "mentions_entity",
				Field: "entities",
			},
		},
	})
	if err != nil {
		return nil, err
	}

	return &graphIndexConfig, nil
}

// createTableWithIndexes creates the table, logs the result, and waits for shards to be ready.
func createTableWithIndexes(ctx context.Context, client *antfly.AntflyClient, tableName string, numShards int, indexes map[string]antfly.IndexConfig) error {
	err := client.CreateTable(ctx, tableName, antfly.CreateTableRequest{
		NumShards: uint(numShards),
		Indexes:   indexes,
	})
	if err != nil {
		log.Printf("Warning: Failed to create table (may already exist): %v\n", err)
	} else {
		indexNames := make([]string, 0, len(indexes))
		for name := range indexes {
			indexNames = append(indexNames, name)
		}
		fmt.Printf("Table created with indexes: %s\n\n", strings.Join(indexNames, ", "))
	}

	return waitForShardsReady(ctx, client, tableName, 30*time.Second)
}

// ANCHOR: batched_linear_merge
// performBatchedLinearMerge performs LinearMerge in batches with progress logging
// Returns the final cursor position after processing all batches
func performBatchedLinearMerge(
	ctx context.Context,
	client *antfly.AntflyClient,
	tableName string,
	records map[string]any,
	batchSize int,
	dryRun bool,
) (string, error) {
	// Sort IDs for deterministic pagination (REQUIRED for linear merge!)
	ids := make([]string, 0, len(records))
	for id := range records {
		ids = append(ids, id)
	}
	// CRITICAL: Must sort IDs for linear merge cursor logic to work correctly
	sortedIDs := make([]string, len(ids))
	copy(sortedIDs, ids)
	// Use slices.Sort from Go 1.21+
	// If on older Go, use: sort.Strings(sortedIDs)
	slices.Sort(sortedIDs)

	totalBatches := (len(sortedIDs) + batchSize - 1) / batchSize
	fmt.Printf("Loading %d documents in %d batches of %d records\n", len(sortedIDs), totalBatches, batchSize)

	cursor := ""
	totalUpserted := 0
	totalSkipped := 0
	totalDeleted := 0

	for batchNum := range totalBatches {
		// Calculate batch range
		start := batchNum * batchSize
		end := min(start+batchSize, len(sortedIDs))

		// Build batch records map
		batchIDs := sortedIDs[start:end]
		batchRecords := make(map[string]any, len(batchIDs))
		for _, id := range batchIDs {
			batchRecords[id] = records[id]
		}

		fmt.Printf("[Batch %d/%d] Merging records %d-%d\n",
			batchNum+1, totalBatches, start+1, end)

		// Execute LinearMerge with aknn sync level
		result, err := client.LinearMerge(ctx, tableName, antfly.LinearMergeRequest{
			Records:      batchRecords,
			LastMergedId: cursor,
			DryRun:       dryRun,
			SyncLevel:    antfly.SyncLevelAknn, // Wait for vector index writes
		})
		if err != nil {
			return "", fmt.Errorf("failed to perform linear merge for batch %d: %w", batchNum+1, err)
		}

		// Update cursor for next batch
		if result.NextCursor != "" {
			cursor = result.NextCursor
		} else {
			// Fallback: use last ID in batch
			cursor = batchIDs[len(batchIDs)-1]
		}

		// Accumulate totals
		totalUpserted += result.Upserted
		totalSkipped += result.Skipped
		totalDeleted += result.Deleted

		// Log batch progress
		fmt.Printf("  Completed in %s - Status: %s\n", result.Took, result.Status)
		fmt.Printf("  Upserted: %d, Skipped: %d, Deleted: %d, Keys scanned: %d\n",
			result.Upserted, result.Skipped, result.Deleted, result.KeysScanned)

		if len(result.Failed) > 0 {
			fmt.Printf("  Failed operations: %d\n", len(result.Failed))
			for i, fail := range result.Failed {
				if i >= 5 {
					fmt.Printf("    ... and %d more\n", len(result.Failed)-i)
					break
				}
				fmt.Printf("    [%d] ID=%s, Error=%s\n", i, fail.Id, fail.Error)
			}
		}
	}

	// Log final totals
	fmt.Printf("\n=== Linear Merge Complete ===\n")
	fmt.Printf("Total batches: %d\n", totalBatches)
	fmt.Printf("Total upserted: %d\n", totalUpserted)
	fmt.Printf("Total skipped: %d\n", totalSkipped)
	fmt.Printf("Total deleted: %d\n", totalDeleted)

	return cursor, nil
}

// ANCHOR_END: batched_linear_merge

// waitForShardsReady polls the table status until shards are ready to accept writes
func waitForShardsReady(ctx context.Context, client *antfly.AntflyClient, tableName string, timeout time.Duration) error {
	fmt.Printf("Waiting for shards to be ready...\n")

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	pollCount := 0

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled while waiting for shards")
		case <-ticker.C:
			pollCount++

			if time.Now().After(deadline) {
				return fmt.Errorf("timeout waiting for shards after %d polls", pollCount)
			}

			// Get table status
			status, err := client.GetTable(ctx, tableName)
			if err != nil {
				fmt.Printf("  [Poll %d] Error getting table status: %v\n", pollCount, err)
				continue
			}

			// Check if we have at least one shard
			if len(status.Shards) > 0 {
				// Wait longer to ensure leader election completes and propagates
				if pollCount >= 6 {
					fmt.Printf("Shards ready after %d polls (~%dms)\n\n", pollCount, pollCount*500)
					return nil
				}
				fmt.Printf("  [Poll %d] Found %d shard(s), waiting for leader status to propagate\n", pollCount, len(status.Shards))
			} else {
				fmt.Printf("  [Poll %d] No shards found yet\n", pollCount)
			}
		}
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "docsaf - Documentation Sync to Antfly\n\n")
		fmt.Fprintf(os.Stderr, "Usage:\n")
		fmt.Fprintf(os.Stderr, "  docsaf prepare [flags]  - Process files and create sorted JSON data\n")
		fmt.Fprintf(os.Stderr, "  docsaf load [flags]     - Load JSON data into Antfly\n")
		fmt.Fprintf(os.Stderr, "  docsaf sync [flags]     - Full pipeline (prepare + load)\n")
		fmt.Fprintf(os.Stderr, "\nCommands:\n")
		fmt.Fprintf(os.Stderr, "  prepare  Process documentation files and save to JSON\n")
		fmt.Fprintf(os.Stderr, "  load     Load prepared JSON data into Antfly table\n")
		fmt.Fprintf(os.Stderr, "  sync     Process files and load directly (original behavior)\n")
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  # Prepare data\n")
		fmt.Fprintf(os.Stderr, "  docsaf prepare --dir /path/to/docs --output docs.json\n\n")
		fmt.Fprintf(os.Stderr, "  # Prepare with NER entity extraction\n")
		fmt.Fprintf(os.Stderr, "  docsaf prepare --dir /path/to/docs --output docs.json \\\n")
		fmt.Fprintf(os.Stderr, "    --ner-model fastino/gliner2-base-v1 \\\n")
		fmt.Fprintf(os.Stderr, "    --ner-label technology --ner-label concept --ner-label api_endpoint\n\n")
		fmt.Fprintf(os.Stderr, "  # Load prepared data\n")
		fmt.Fprintf(os.Stderr, "  docsaf load --input docs.json --table docs --create-table\n\n")
		fmt.Fprintf(os.Stderr, "  # Full pipeline with NER + knowledge graph\n")
		fmt.Fprintf(os.Stderr, "  docsaf sync --dir /path/to/docs --table docs --create-table \\\n")
		fmt.Fprintf(os.Stderr, "    --ner-model fastino/gliner2-base-v1 \\\n")
		fmt.Fprintf(os.Stderr, "    --ner-label technology --ner-label concept\n\n")
		os.Exit(1)
	}

	var err error
	switch os.Args[1] {
	case "prepare":
		err = prepareCmd(os.Args[2:])
	case "load":
		err = loadCmd(os.Args[2:])
	case "sync":
		err = syncCmd(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", os.Args[1])
		fmt.Fprintf(os.Stderr, "Valid commands: prepare, load, sync\n")
		os.Exit(1)
	}

	if err != nil {
		log.Fatalf("Error: %v", err)
	}
}
