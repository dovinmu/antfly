# Epstein Documents Search

A complete example demonstrating how to use Antfly to index and search the publicly released Jeffrey Epstein court documents and DOJ files.

## Overview

This tool downloads, processes, and indexes PDF documents from:

1. **January 2024 Court Unsealing** - 943 pages from Giuffre v. Maxwell (Case 1:15-cv-07433-LAP)
2. **DOJ December 2025 Release** - 4,055+ documents across 8 datasets released via the Epstein Files Transparency Act (EFTA)
3. **DOJ January 2026 Release** - 3.5 million+ pages across datasets 10-12 (dataset 9 excluded due to incomplete release)

The documents are processed page-by-page, chunked for semantic search, and made searchable through both BM25 (full-text) and vector similarity search.

## Quick Start

### Prerequisites

- Go 1.21+
- Zig 0.16.0+
- Running Zig Antfly standalone with Antfly inference models

### 1. Build and Start Zig Antfly

```bash
# From the antfly root directory
cd zig
zig build antfly
export ANTFLY_BIN="$PWD/zig-out/bin/antfly"
./zig-out/bin/antfly standalone
```

This starts a single-node Antfly cluster on `http://localhost:8080` with the
Zig inference routes mounted under the same public API.

### 2. Pull Models

The Zig model pull path treats HuggingFace as the default source, so `hf:` is
omitted here.

```bash
cd zig
./zig-out/bin/antfly inference pull antflydb/clipclap:gguf:Q4_K --tasks embed
./zig-out/bin/antfly inference pull antflydb/gliner2-base-v1-q4_k --tasks extract --capabilities extraction
./zig-out/bin/antfly inference pull antflydb/Florence-2-base --tasks read
./zig-out/bin/antfly inference pull ggml-org/gemma-4-e2b-it-gguf:gguf:Q4_0 --tasks generate

# Optional native reranker for the search UI.
./zig-out/bin/antfly inference pull ggml-org/Qwen3-Reranker-0.6B-Q8_0-GGUF:gguf:Q8_0 --tasks rerank
```

### 3. Build the Tool

```bash
cd examples/epstein
go build -o epstein .
```

### 4. Download or Point at Local Documents

Choose a dataset based on your needs:

```bash
# Option A: January 2024 Court Documents (~23MB, 943 pages)
# Good for testing and smaller deployments
./epstein download --dataset court-2024

# Option B: DOJ December 2025 Release (~4.8GB, 8 datasets)
./epstein download --dataset doj-complete

# Option C: DOJ January 2026 Release (~104GB, datasets 10-12)
# These are ZIP archives that are automatically extracted after download
./epstein download --dataset doj-jan2026

# Option D: All configured archive presets, not the entire current DOJ library
./epstein download --dataset all
```

If the files are already on an external disk, point the tool at them instead:

```bash
export EPSTEIN_DOCS_DIR="/path/to/T9/epstein-files"
export EPSTEIN_ZIP="/path/to/T9/DataSet_10.zip"
```

### 5. Prepare, Enrich, and Load

```bash
# Process PDFs into page records. --split-pages enables direct PDF viewing.
./epstein prepare --dir "$EPSTEIN_DOCS_DIR" --split-pages

# Small smoke run from a large local disk: process only the first few PDFs and pages.
./epstein prepare --dir /Volumes/T9/epstein-files --split-pages \
  --limit-files 2 --limit-pages 50 \
  --output epstein-smoke.json

# Recover empty, identifier-only, short, fragmented, and corrupt pages natively.
# ANTFLY_BIN must point to a build with `pdf render-page`.
./epstein enrich --input epstein-docs.json --dir "$EPSTEIN_DOCS_DIR" \
  --checkpoint-every 25

# Optional: add entity metadata to each page record.
./epstein entities --input epstein-docs-enriched.json

# Load into Antfly using ClipClap embeddings.
./epstein load --input epstein-docs-enriched-entities.json --create-table \
  --sync-level full_index

# Load the small smoke file and enable the Zig artifact-backed relation graph.
./epstein load --input epstein-smoke.json --table epstein_smoke --create-table \
  --enable-artifact-graph \
  --artifact-producer extractor \
  --artifact-extractor-model antflydb/gliner2-base-v1-q4_k

# Optional: use the slower Gemma tool-call generator path for richer relation extraction.
./epstein load --input epstein-smoke.json --table epstein_smoke_gemma --create-table \
  --enable-artifact-graph \
  --artifact-producer generator \
  --artifact-extractor-model ggml-org/gemma-4-E4B-it-GGUF
```

For ZIP sources that should not be extracted first:

```bash
./epstein prepare --zip "$EPSTEIN_ZIP" --split-pages
./epstein entities --input epstein-docs.json
./epstein load --input epstein-docs-entities.json --create-table
```

### 6. Start Search Interface

```bash
./epstein serve
```

Open http://localhost:3000 in your browser.

## Commands

### `download`

Downloads documents from Internet Archive.

```bash
./epstein download [flags]

Flags:
  --output    Output directory (default: ./epstein-docs)
  --dataset   Dataset to download: court-2024, doj-complete, doj-jan2026, all
```

### `prepare`

Processes PDF files and creates JSON data for loading.

```bash
./epstein prepare [flags]

Flags:
  --dir       Path to PDF directory (default: ./epstein-docs)
  --output    Output JSON file (default: epstein-docs.json)
  --base-url  Base URL for document links
  --split-pages
              Split PDFs into individual page PDFs for direct viewing
  --zip       Path to ZIP archive containing PDFs (repeatable)
  --enable-ocr
              Enable OCR fallback through Antfly inference readers
  --ocr-url   Antfly inference URL (default: ANTFLY_INFERENCE_URL or http://localhost:8080)
  --ocr-models
              OCR models to try (default: antflydb/Florence-2-base)
  --limit-files
              Process at most this many source PDFs (default: all)
  --limit-pages
              Keep at most this many parsed pages in output (default: all)
```

Split-page preparation retains pages with no extracted text so native enrichment
can recover them. Text-extraction failures retain a source-backed record with
`metadata.text_extraction_error`; preparation writes the records and exits
nonzero rather than silently dropping those pages.

### `load`

Loads prepared JSON data into Antfly.

```bash
./epstein load [flags]

Flags:
  --url             Antfly API URL (default: http://localhost:8080/db/v1)
  --table           Table name (default: epstein_docs)
  --input           Input JSON file (default: epstein-docs.json)
  --create-table    Create table if it doesn't exist
  --dry-run         Preview changes without applying
  --num-shards      Number of shards (default: 1)
  --batch-size      Batch size for linear merge (default: 25)
  --sync-level      write, full_text, full_index, enrichments, or propose (default: write)
  --inference-url     Antfly inference root URL for chunking; table config stores its /ai/v1 route (default: ANTFLY_INFERENCE_URL or http://localhost:8080)
  --embedding-model Embedding model (default: antflydb/clipclap)
  --chunker-model   Chunker model (default: fixed-bert-tokenizer)
  --target-tokens   Target tokens per chunk (default: 512)
  --overlap-tokens  Overlap between chunks (default: 50)
  --enable-artifact-graph
                    Create an artifact-backed autograph relation graph index
  --artifact-graph-index
                    Graph index name (default: autograph_relations)
  --artifact-name   Generated asset artifact name (default: relations_v1)
  --artifact-producer
                    Artifact producer type: extractor or generator
                    (default: extractor)
  --artifact-extractor-model
                    Antfly model for artifact relation extraction
                    (default: antflydb/gliner2-base-v1-q4_k)
  --artifact-labels Entity labels for the artifact extractor
  --artifact-relation-labels
                    Relation labels for the artifact extractor
  --autoschema      Provision the AutoSchemaKG pipeline: LLM extraction
                    passes, entities/events resolution, and a concept taxonomy
                    (requires --create-table; see "AutoSchemaKG mode" below)
  --autoschema-model
                    Antfly generative model shared by the AutoSchemaKG
                    extractors and conceptualizer
                    (default: ggml-org/gemma-4-E4B-it-GGUF)
  --autoschema-gliner-model
                    GLiNER2.5 extraction model for the fast closed-schema
                    autoschema lane; "" disables the lane
                    (default: fastino/gliner2.5-base-v1)
```

Managed embedding dimensions are detected by Antfly from the chosen model;
they are not fixed to ClipClap's dimension. For native text-only BGE indexing:

```bash
./epstein load --input epstein-docs-enriched.json --create-table \
  --embedding-model BAAI/bge-small-en-v1.5 --chunker-model fixed_bert \
  --target-tokens 448 --overlap-tokens 50 --sync-level full_index
```

Pull the selected model into the serving runtime's model directory first.
The example configures at most 256 chunks per document and partial coverage.
Inspect index readiness and coverage, including skipped or failed sources;
successful writes alone do not establish complete semantic indexing.

### AutoSchemaKG mode

`--autoschema` provisions the schema-free knowledge graph pipeline described
in `zig/AUTOSCHEMA.md`, following *AutoSchemaKG: Autonomous Knowledge Graph
Construction through Dynamic Schema Induction from Web-Scale Corpora*
(arXiv:2505.23628). It coexists with `--enable-artifact-graph` and creates:

- Two generator asset enrichments on the documents table, one per
  extraction pass (`kg_ee_v1` entity-entity; `kg_events_v1` events with
  their participating entities and the temporal/causal relations between
  them), each a forced tool call emitting the `extraction_graph` artifact
  shape.
- A `knowledge_graph` graph index merging the artifact streams, with
  label-routed resolvers that promote `event`-labelled mentions into an
  `events` table (compositional `event/{{ hash _entity.event_identity }}`
  keys: sorted participant slugs + predicate lemma, composed from the
  participants' canonical entity keys once their resolution lands) and
  every other mention into an `entities` table
  (label-free `entity/{{ slug _entity.text }}` keys; the extractor's label
  rides the promoted document as `entity_type`, never the key, so mentions
  labeled differently by different passes still converge on one node).
- A third, GLiNER2.5-powered extraction lane (`kg_gliner_v1`, disable with
  `--autoschema-gliner-model ""`): a fast `extractor` asset producer with a
  closed entity/relation schema (person, organization, location, date;
  `employed_by`, `located_in`, `traveled_with`, `met_with`,
  `associated_with`) feeding the same `knowledge_graph` index as an
  `extraction_relation` source. GLiNER relations reference entities
  positionally (`entity_index`); the lane's own catch-all resolver mints the
  SAME label-free `entity/{{ slug _entity.text }}` canonical keys as the
  LLM lanes, so both extractors' edges converge on shared entity nodes
  regardless of how each extractor labeled the mention, and a
  `min_confidence` floor drops low-score junk before it mints nodes.
  The LLM stages keep the paper's open-vocabulary verb-phrase relations,
  which a closed-schema extractor cannot express.
- A recursive conceptualization autograph on the `entities` table: the
  `conceptualize_v1` enrichment abstracts each promoted entity into three or
  more concept phrases (grounded by `neighbor_context` sampling of the
  taxonomy graph), and the `taxonomy` graph index promotes them into a
  `concepts` table with `is_a` edges.

```bash
./epstein load --create-table --autoschema \
  --autoschema-model ggml-org/gemma-4-E4B-it-GGUF \
  --input epstein-docs-small.json
```

The `entities`, `events`, and `concepts` tables are created automatically
before the documents table so cross-table promotion targets exist up front.

### `sync`

Full pipeline: process PDFs and load directly.

```bash
./epstein sync [flags]

# Combines prepare + load flags
```

### `enrich`

Runs native PDF rendering, OCR, and visual description over prepared JSON.
Set `ANTFLY_BIN` to an explicit Antfly executable with `pdf render-page`; no Go
renderer, platform renderer, or degraded-render result is accepted by this path.

```bash
./epstein enrich [flags]

Flags:
  --input       Input JSON file (default: epstein-docs.json)
  --output      Output JSON file (default: {input-base}-enriched.json)
  --inference-url Antfly inference URL (default: ANTFLY_INFERENCE_URL or http://localhost:8080)
  --model       OCR model (default: ggml-org/gemma-4-e2b-it-gguf:gguf:Q4_0)
  --ocr-mode    generate or read (default: generate)
  --vision-model Visual description model (default: same Gemma model; "" disables)
  --fallback-reader-model Native reader fallback (default: antflydb/Florence-2-base; "" disables)
  --category    ocr, vision, quality, or all (default: all)
  --min-content Short-content threshold (default: 50)
  --only-empty-or-identifier Restrict selection to the earlier narrow recovery pass
  --reprocess   Revisit previously enriched pages using their original content
  --dpi         Native raster resolution, integer 72–600 (default: 200)
  --max-tokens  OCR generation limit (default: 2048)
  --vision-max-tokens Visual description limit (default: 256)
  --fallback-max-tokens Reader fallback limit (default: 768)
  --workers     Concurrent enrichment workers (default: 1)
  --checkpoint-every Atomically save after this many results (default: 100)
  --dir         Base directory for resolving source PDFs
  --zip         Source ZIP archive for page lookup (repeatable)
  --dry-run     Report candidate count without inference
```

The default pass includes short fragments such as `J`, symbol-only extraction,
fragmented text, and font-corrupt text, not just blank or identifier-only pages.
Already enriched pages are skipped unless `--reprocess` is supplied.
Recovered text and separately labeled visual descriptions become searchable
`content`; the bad extraction remains in `original_content`, not in the search
field. Source PDFs are not changed.

`metadata.recovery` records PDF/PNG SHA-256 hashes, selection reasons, native
rendering, actual stage models, prompts, token limits, and fallback errors.
Outputs remain **machine-generated and unreviewed**, not authoritative
transcriptions. Truncated generation is rejected before the optional native
reader fallback. OCR fallback can still lose layout or misread dense tables.
Failed pages retain error metadata and the command exits nonzero after saving
successful work. For the default pass, resume with the checkpoint as `--input`;
successful pages are skipped and failed pages remain candidates. `--reprocess`
forces eligible enriched pages through recovery again; do not use it merely to
resume a normal pass.

For a standalone renderer smoke:

```bash
"$ANTFLY_BIN" pdf render-page source.pdf --page 1 --dpi 200 \
  --profile ocr --require-native --out page.png
```

The CLI reports effective geometry and render quality on stderr. Native-only
rendering forbids platform fallback; enrichment additionally rejects degraded
quality. Qualify the inference runtime with real page images before a bulk run,
not just model inventory checks. The inference server's admission limit is
weighted by image working memory: `--max-concurrent-requests 1` can reject a
single page image. Use `--workers 1` to serialize enrichment instead.

On macOS arm64, the 2026-10-01 qualification found a Gemma image-generation
segfault in native build `main-8c295b2b49-dirty`; a previously qualified native reader build
completed the same pages. Renderer, inference, and search executables may be
pinned independently. Do not silently substitute a failed or truncated OCR
response for a recovered page.

### `entities`

Adds Antfly inference NER metadata to prepared or enriched JSON records.

```bash
./epstein entities [flags]

Flags:
  --input           Input JSON file (default: epstein-docs.json)
  --output          Output JSON file (default: {input-base}-entities.json)
  --inference-url     Antfly inference URL (default: ANTFLY_INFERENCE_URL or http://localhost:8080)
  --model           Recognizer model (default: antflydb/gliner2-base-v1-q4_k)
  --labels          Entity labels to extract
  --relation-labels Relation labels to extract (default: associated with, communicated with, traveled to, visited, worked for, represented by, mentioned in, located in)
  --batch-size      Text windows per Antfly inference recognize request (default: 16)
  --max-chars       Maximum characters per recognizer window (default: 3000)
  --overlap-chars   Characters of overlap between recognizer windows (default: 300)
  --reprocess       Re-process records that already have entities
```

### `serve`

Starts a web server with search interface.

```bash
./epstein serve [flags]

Flags:
  --url     Antfly API URL (default: http://localhost:8080/db/v1)
  --table   Table name to search (default: epstein_docs)
  --listen  Listen address (default: :3000)
  --pdf-dir Root containing the prepared pages/ directory (default: ./epstein-docs)
  --reranker-model Native Antfly reranker model (default: disabled)
  --reranker-candidates Candidate window, at least 20 (default: 50)
```

For native reranking after pulling the model:

```bash
./epstein serve --pdf-dir "$EPSTEIN_DOCS_DIR" \
  --reranker-model ggml-org/Qwen3-Reranker-0.6B-Q8_0-GGUF:gguf:Q8_0
```

## Architecture

### Document Processing

1. **PDF Extraction**: Uses `ledongthuc/pdf` to extract text page-by-page
2. **Document Sections**: Each page becomes a `DocumentSection` with:
   - Unique ID (hash of file path + page number)
   - Title (document title + page number)
   - Content (extracted text)
   - Metadata (page number, total pages, PDF metadata)

### Indexing

Documents are indexed with:

1. **BM25 Full-Text Index** (automatic)
   - Keyword search
   - Exact phrase matching

2. **Embedding Index** (`embeddings`)
   - Semantic similarity search
   - Powered by the native Zig Antfly embedder + `antflydb/clipclap`
   - Chunked with configurable overlap

3. **Entity Metadata** (optional)
   - `epstein entities` stores recognized entities in `metadata.entities`
   - Optional relations are stored in `metadata.relations`
   - Long pages are processed as overlapping windows and offsets are rebased to the page
   - Failed entity windows are tracked and retried on the next run
   - The search UI displays entity chips when present

### Search

Both the web interface and JSON search API issue the same hybrid request:
- Explicit full-text matching on `content`, plus semantic search with only
  `embeddings` in the semantic index list.
- Optional native reranking of the candidate window.
- Up to 20 highest-ranked results. Scores are query-relative ranking signals,
  not calibrated confidence; weak or unrelated queries can still return results.
  No universal score cutoff or heuristic short-text filter is applied.

The relation panel checks whether `autograph_relations` exists and is queryable.
It distinguishes `not_configured`, `ready_empty`, `ready`, and `error`. Traversal
starts from content matches for the actual query; an empty result never triggers
an unrelated sample graph. Entity metadata by itself does not populate this
index. Native main `8c295b2b49` returned `InvalidGraphEdgesResponse` in a
configured-graph smoke; this is surfaced as an error, not an empty graph.

## Datasets

These archive presets describe historical releases. The
[current official DOJ catalog](https://www.justice.gov/epstein/doj-disclosures)
lists numbered datasets **1–12**, plus court, FOIA, and prior-disclosure
collections. In particular, dataset 9 is not covered by the example's older
DOJ 1–8 / 10–12 split. `--dataset all` is not a current-library completeness
guarantee. Maintain a source URL/hash manifest and reconcile downloaded PDFs,
split pages, prepared records, and index coverage before declaring a corpus
complete.

### January 2024 Court Unsealing

- **Source**: [Internet Archive](https://archive.org/details/final-epstein-documents)
- **Size**: ~23MB (PDF), 943 pages
- **Content**: Unsealed documents from Giuffre v. Maxwell civil case
- **Released**: January 3, 2024

### DOJ Complete Release

- **Source**: [Internet Archive](https://archive.org/details/combined-all-epstein-files)
- **Size**: ~4.8GB (8 consolidated PDFs)
- **Content**: 4,055+ documents released under EFTA
- **Released**: December 19, 2025
- **Datasets**:
  - DataSet 1: 1.2GB
  - DataSet 2: 629MB
  - DataSet 3: 598MB
  - DataSet 4: 356MB
  - DataSet 5: 61MB
  - DataSet 6: 53MB
  - DataSet 7: 98MB
  - DataSet 8: 1.8GB

## Performance

Expected processing times (approximate):

| Dataset | Size | Prepare | Load | Embeddings |
|---------|------|---------|------|------------|
| court-2024 | ~23MB | 30s | 2min | 20-30min |
| doj-complete | ~4.8GB | 10min | 30min | 4-8hrs |

Embedding generation is the slowest step as each chunk needs to be processed by the ML model.

## API Usage

You can also query the Antfly API directly:

```bash
# Search via curl
curl "http://localhost:8080/db/v1/tables/epstein_docs/query" \
  -H "Content-Type: application/json" \
  -d '{
    "semantic_search": "flight logs to Little St James",
    "indexes": ["full_text_index_v0", "embeddings"],
    "limit": 10
  }'
```

Or use the Go SDK:

```go
client, _ := antfly.NewAntflyClient("http://localhost:8080/db/v1", http.DefaultClient)

resp, _ := client.Query(ctx, "epstein_docs", antfly.QueryRequest{
    SemanticSearch: "flight logs",
    Indexes:        []string{"full_text_index_v0", "embeddings"},
    Limit:          10,
})

for _, hit := range resp.Hits {
    fmt.Printf("Score: %.2f - %s\n", hit.Score, hit.Document["title"])
}
```

## Troubleshooting

### "No PDF files found"

Make sure you've run `download` first and the PDFs are in the expected directory.

### "Failed to create table (may already exist)"

The table already exists. This is fine - the sync will update existing documents.

### Slow embedding generation

Embedding generation runs asynchronously. You can monitor progress:

```bash
curl "http://localhost:8080/db/v1/tables/epstein_docs/indexes/embeddings" | jq '.status.total_indexed'
```

### Out of memory

For large datasets, increase the Go garbage collector threshold:

```bash
GOGC=50 ./epstein sync --create-table
```

Or use multiple shards:

```bash
./epstein sync --create-table --num-shards 4
```

## Legal Notice

These documents are publicly available through official government channels and public archives. This tool is provided for research, journalism, and educational purposes. The creators of this tool do not endorse or condone any illegal activity.

## References

- [DOJ Epstein Library](https://www.justice.gov/epstein)
- [Internet Archive - Epstein Documents](https://archive.org/details/combined-all-epstein-files)
- [PDF Association Analysis](https://pdfa.org/a-case-study-in-pdf-forensics-the-epstein-pdfs/)
