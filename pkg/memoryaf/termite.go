package memoryaf

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"sync"
	"time"

	"go.uber.org/zap"
)

// TermiteClient calls the Termite NER API for named entity recognition.
type TermiteClient struct {
	baseURL    string
	nerModel   string
	nerLabels  []string
	httpClient *http.Client
	logger     *zap.Logger

	mu        sync.Mutex
	available *bool
	checkedAt time.Time
}

// NewTermiteClient creates a Termite NER client.
func NewTermiteClient(baseURL, nerModel string, nerLabels []string, logger *zap.Logger) *TermiteClient {
	return &TermiteClient{
		baseURL:   baseURL,
		nerModel:  nerModel,
		nerLabels: nerLabels,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		logger: logger,
	}
}

// DefaultTermiteClient creates a TermiteClient with default settings.
func DefaultTermiteClient(logger *zap.Logger) *TermiteClient {
	return NewTermiteClient(
		"http://localhost:11433",
		"fastino/gliner2-base-v1",
		[]string{"person", "organization", "project", "technology", "service", "tool", "framework", "pattern"},
		logger,
	)
}

type recognizeRequest struct {
	Model  string   `json:"model"`
	Texts  []string `json:"texts"`
	Labels []string `json:"labels"`
}

type recognizeResponse struct {
	Entities [][]rawEntity `json:"entities"`
}

type rawEntity struct {
	Text  string  `json:"text"`
	Label string  `json:"label"`
	Score float64 `json:"score"`
}

func (c *TermiteClient) isAvailable(ctx context.Context) bool {
	c.mu.Lock()
	if c.available != nil && time.Since(c.checkedAt) < 60*time.Second {
		avail := *c.available
		c.mu.Unlock()
		return avail
	}
	c.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/models", nil)
	if err != nil {
		c.setAvailable(false)
		return false
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.setAvailable(false)
		return false
	}
	resp.Body.Close()

	avail := resp.StatusCode == http.StatusOK
	c.setAvailable(avail)
	return avail
}

func (c *TermiteClient) setAvailable(v bool) {
	c.mu.Lock()
	c.available = &v
	c.checkedAt = time.Now()
	c.mu.Unlock()
}

// RecognizeEntities calls Termite GLiNER2 to extract named entities.
// Returns empty slice if Termite is unavailable (graceful degradation).
func (c *TermiteClient) RecognizeEntities(ctx context.Context, text string) []Entity {
	if !c.isAvailable(ctx) {
		return nil
	}

	body, err := json.Marshal(recognizeRequest{
		Model:  c.nerModel,
		Texts:  []string{text},
		Labels: c.nerLabels,
	})
	if err != nil {
		c.logger.Warn("failed to marshal NER request", zap.Error(err))
		return nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/recognize", bytes.NewReader(body))
	if err != nil {
		c.logger.Warn("failed to create NER request", zap.Error(err))
		return nil
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.logger.Warn("Termite NER request failed", zap.Error(err))
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		c.logger.Warn("Termite NER failed", zap.Int("status", resp.StatusCode))
		return nil
	}

	var result recognizeResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		c.logger.Warn("failed to decode NER response", zap.Error(err))
		return nil
	}

	if len(result.Entities) == 0 || len(result.Entities[0]) == 0 {
		return nil
	}

	var entities []Entity
	for _, e := range result.Entities[0] {
		entities = append(entities, Entity{
			Text:  e.Text,
			Label: e.Label,
			Score: e.Score,
		})
	}
	return entities
}

// TermiteURL derives the Termite URL from an Antfly cluster URL.
// Termite runs on port 11433 on the same host as Antfly.
func TermiteURL(antflyURL string) string {
	u, err := url.Parse(antflyURL)
	if err != nil || u.Hostname() == "" {
		return "http://localhost:11433"
	}
	u.Host = u.Hostname() + ":11433"
	u.Path = ""
	return u.String()
}
