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

package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	antfly "github.com/antflydb/antfly/go/pkg/sdk"
)

func TestGraphEndpointReportsNotConfigured(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/db/v1/tables/documents/indexes" {
			t.Errorf("unexpected backend request: %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[]`))
	}))
	defer backend.Close()

	server := newGraphContractServer(t, backend)
	response := httptest.NewRecorder()
	server.handleAPIGraph(response, httptest.NewRequest(http.MethodGet, "/api/graph?q=Maxwell", nil))

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", response.Code, http.StatusOK, response.Body.String())
	}
	var graph GraphVisualization
	if err := json.NewDecoder(response.Body).Decode(&graph); err != nil {
		t.Fatalf("decode graph response: %v", err)
	}
	if graph.State != graphStateNotConfigured || graph.Message == "" || len(graph.Nodes) != 0 || len(graph.Edges) != 0 {
		t.Fatalf("graph response = %#v, want explicit not-configured state", graph)
	}
}

func TestGraphEndpointReportsInspectionError(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "catalog unavailable", http.StatusServiceUnavailable)
	}))
	defer backend.Close()

	server := newGraphContractServer(t, backend)
	response := httptest.NewRecorder()
	server.handleAPIGraph(response, httptest.NewRequest(http.MethodGet, "/api/graph?q=Maxwell", nil))

	if response.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d: %s", response.Code, http.StatusBadGateway, response.Body.String())
	}
	var graph GraphVisualization
	if err := json.NewDecoder(response.Body).Decode(&graph); err != nil {
		t.Fatalf("decode graph response: %v", err)
	}
	if graph.State != graphStateError || graph.Message == "" {
		t.Fatalf("graph response = %#v, want explicit error state", graph)
	}
}

func TestGraphEndpointReportsReadyEmptyWithoutSampling(t *testing.T) {
	queryCalls := 0
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/db/v1/tables/documents/indexes":
			_, _ = w.Write([]byte(`[{
				"config":{"name":"autograph_relations","type":"graph"},
				"shard_status":{},
				"status":{
					"index_type":"graph",
					"milestones":{
						"queryable":{"reached":true,"blockers":[]},
						"complete":{"reached":true,"blockers":[]}
					}
				}
			}]`))
		case r.Method == http.MethodPost && r.URL.Path == "/db/v1/tables/documents/query":
			queryCalls++
			_, _ = w.Write([]byte(`{"responses":[{"hits":{"hits":[],"max_score":0,"total":{"value":0,"relation":"eq"}},"graph_results":{"relations":{"kind":"nodes","nodes":[],"stats":{"returned_items":0,"truncated":false}}},"status":200,"took":0}]}`))
		default:
			t.Errorf("unexpected backend request: %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer backend.Close()

	server := newGraphContractServer(t, backend)
	response := httptest.NewRecorder()
	server.handleAPIGraph(response, httptest.NewRequest(http.MethodGet, "/api/graph?q=Maxwell", nil))

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", response.Code, http.StatusOK, response.Body.String())
	}
	var graph GraphVisualization
	if err := json.NewDecoder(response.Body).Decode(&graph); err != nil {
		t.Fatalf("decode graph response: %v", err)
	}
	if graph.State != graphStateReadyEmpty || graph.Message == "" {
		t.Fatalf("graph response = %#v, want ready-empty state", graph)
	}
	if queryCalls != 1 {
		t.Fatalf("graph query calls = %d, want exactly one query without unrelated sampling", queryCalls)
	}
}

func newGraphContractServer(t *testing.T, backend *httptest.Server) *SearchServer {
	t.Helper()
	client, err := antfly.NewAntflyClient(backend.URL, backend.Client())
	if err != nil {
		t.Fatalf("NewAntflyClient: %v", err)
	}
	return &SearchServer{
		client:    client,
		antflyURL: backend.URL + "/db/v1",
		tableName: "documents",
	}
}

func TestGraphVisualizationRejectsMissingOrMalformedResults(t *testing.T) {
	for _, payload := range []string{
		`{}`,
		`{"responses":[{}]}`,
		`{"responses":[{"graph_results":{"relations":{"kind":"unexpected"}}}]}`,
	} {
		var response antfly.QueryResponses
		if err := json.Unmarshal([]byte(payload), &response); err != nil {
			t.Fatal(err)
		}
		graph := buildGraphVisualization("bear", &response)
		if graph.State != graphStateError {
			t.Fatalf("malformed response reported as %q instead of an error: %s", graph.State, payload)
		}
	}
}
