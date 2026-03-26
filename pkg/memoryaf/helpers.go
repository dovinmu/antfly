package memoryaf

import (
	"encoding/json"
	"fmt"
)

// mustMarshal marshals v to JSON or panics.
func mustMarshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("memoryaf: marshal failed: %v", err))
	}
	return b
}

// hit is a parsed query hit.
type hit struct {
	ID     string
	Source map[string]any
	Score  float64
}

// queryResponse is the raw Antfly query response envelope.
type queryResponse struct {
	Responses []struct {
		Hits struct {
			Hits  []rawHit `json:"hits"`
			Total uint64   `json:"total"`
		} `json:"hits"`
		Aggregations map[string]aggregationResult `json:"aggregations"`
		Error        string                       `json:"error"`
	} `json:"responses"`
}

type rawHit struct {
	ID     string         `json:"_id"`
	Score  float64        `json:"_score"`
	Source map[string]any `json:"_source"`
}

type aggregationResult struct {
	Buckets []aggregationBucket `json:"buckets"`
}

type aggregationBucket struct {
	Key      string `json:"key"`
	DocCount int    `json:"doc_count"`
}

func parseResponse(data []byte) (*queryResponse, error) {
	var resp queryResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}
	if len(resp.Responses) > 0 && resp.Responses[0].Error != "" {
		return nil, fmt.Errorf("query error: %s", resp.Responses[0].Error)
	}
	return &resp, nil
}

func firstHitFromResponse(data []byte) (*hit, error) {
	resp, err := parseResponse(data)
	if err != nil {
		return nil, err
	}
	if len(resp.Responses) == 0 || len(resp.Responses[0].Hits.Hits) == 0 {
		return nil, nil
	}
	h := resp.Responses[0].Hits.Hits[0]
	return &hit{ID: h.ID, Source: h.Source, Score: h.Score}, nil
}

func hitsFromResponse(data []byte) ([]hit, error) {
	resp, err := parseResponse(data)
	if err != nil {
		return nil, err
	}
	if len(resp.Responses) == 0 {
		return nil, nil
	}
	var hits []hit
	for _, h := range resp.Responses[0].Hits.Hits {
		hits = append(hits, hit{ID: h.ID, Source: h.Source, Score: h.Score})
	}
	return hits, nil
}

func statsFromResponse(data []byte) (*MemoryStats, error) {
	resp, err := parseResponse(data)
	if err != nil {
		return nil, err
	}
	if len(resp.Responses) == 0 {
		return &MemoryStats{
			ByType:       map[string]int{},
			ByProject:    map[string]int{},
			ByTag:        map[string]int{},
			ByVisibility: map[string]int{},
		}, nil
	}

	r := resp.Responses[0]
	return &MemoryStats{
		TotalMemories: int(r.Hits.Total),
		ByType:        bucketsToMap(r.Aggregations["by_type"]),
		ByProject:     bucketsToMap(r.Aggregations["by_project"]),
		ByTag:         bucketsToMap(r.Aggregations["by_tag"]),
		ByVisibility:  bucketsToMap(r.Aggregations["by_visibility"]),
	}, nil
}

func bucketsToMap(agg aggregationResult) map[string]int {
	m := make(map[string]int, len(agg.Buckets))
	for _, b := range agg.Buckets {
		m[b.Key] = b.DocCount
	}
	return m
}

// --- Field extraction helpers ---

func getString(m map[string]any, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func getStringDefault(m map[string]any, key, def string) string {
	if s := getString(m, key); s != "" {
		return s
	}
	return def
}

func getInt(m map[string]any, key string) int {
	if v, ok := m[key]; ok {
		switch n := v.(type) {
		case float64:
			return int(n)
		case int:
			return n
		}
	}
	return 0
}

func getStringSlice(m map[string]any, key string) []string {
	v, ok := m[key]
	if !ok {
		return nil
	}
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	var out []string
	for _, item := range arr {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func getEntities(m map[string]any, key string) []Entity {
	v, ok := m[key]
	if !ok {
		return nil
	}
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	var out []Entity
	for _, item := range arr {
		em, ok := item.(map[string]any)
		if !ok {
			continue
		}
		out = append(out, Entity{
			Text:  getString(em, "text"),
			Label: getString(em, "label"),
			Score: getFloat(em, "score"),
		})
	}
	return out
}

func getFloat(m map[string]any, key string) float64 {
	if v, ok := m[key]; ok {
		if f, ok := v.(float64); ok {
			return f
		}
	}
	return 0
}

// buildMemoryDoc creates a document map from StoreMemoryArgs.
func buildMemoryDoc(args StoreMemoryArgs, userID, now string) map[string]any {
	doc := map[string]any{
		"content":     args.Content,
		"memory_type": coalesce(args.MemoryType, "semantic"),
		"tags":        args.Tags,
		"project":     args.Project,
		"source":      args.Source,
		"created_by":  userID,
		"visibility":  coalesce(args.Visibility, "team"),
		"updated_at":  now,
		"entities":    []any{},
	}
	if args.Tags == nil {
		doc["tags"] = []string{}
	}
	if args.EventTime != "" {
		doc["event_time"] = args.EventTime
	}
	if args.Context != "" {
		doc["context"] = args.Context
	}
	if args.Confidence != nil {
		doc["confidence"] = *args.Confidence
	}
	if args.Supersedes != "" {
		doc["supersedes"] = args.Supersedes
	}
	if args.Trigger != "" {
		doc["trigger"] = args.Trigger
	}
	if args.Steps != nil {
		doc["steps"] = args.Steps
	}
	if args.Outcome != "" {
		doc["outcome"] = args.Outcome
	}
	return doc
}

// mergeMemoryFields merges update args into an existing memory, returning the updated doc.
func mergeMemoryFields(existing *Memory, args UpdateMemoryArgs, now string) map[string]any {
	doc := map[string]any{
		"content":     coalesce(args.Content, existing.Content),
		"memory_type": coalesce(args.MemoryType, existing.MemoryType),
		"tags":        coalesceSlice(args.Tags, existing.Tags),
		"project":     coalesce(args.Project, existing.Project),
		"source":      coalesce(args.Source, existing.Source),
		"created_by":  existing.CreatedBy,
		"visibility":  coalesce(args.Visibility, existing.Visibility),
		"created_at":  existing.CreatedAt,
		"updated_at":  now,
		"entities":    entitiesToSlice(existing.Entities),
	}

	// Merge type-specific fields
	doc["event_time"] = coalesce(args.EventTime, existing.EventTime)
	doc["context"] = coalesce(args.Context, existing.Context)
	doc["supersedes"] = coalesce(args.Supersedes, existing.Supersedes)
	doc["trigger"] = coalesce(args.Trigger, existing.Trigger)
	doc["outcome"] = coalesce(args.Outcome, existing.Outcome)
	if args.Steps != nil {
		doc["steps"] = args.Steps
	} else if existing.Steps != nil {
		doc["steps"] = existing.Steps
	}
	if args.Confidence != nil {
		doc["confidence"] = *args.Confidence
	} else if existing.Confidence != nil {
		doc["confidence"] = *existing.Confidence
	}

	return doc
}

func coalesce(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func coalesceSlice(a, b []string) []string {
	if a != nil {
		return a
	}
	return b
}

func entitiesToSlice(entities []Entity) []any {
	if len(entities) == 0 {
		return []any{}
	}
	out := make([]any, len(entities))
	for i, e := range entities {
		out[i] = map[string]any{
			"text":  e.Text,
			"label": e.Label,
			"score": e.Score,
		}
	}
	return out
}
