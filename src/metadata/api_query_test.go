// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license

package metadata

import (
	jsonpkg "encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeDenseEmbedding(t *testing.T, values ...float32) Embedding {
	t.Helper()
	var emb Embedding
	require.NoError(t, emb.FromEmbedding0(Embedding0(values)))
	return emb
}

func TestQueryRequestToRemoteIndexQuery_DoesNotInjectMatchAllForEmbeddingsOnly(t *testing.T) {
	req := &QueryRequest{
		Embeddings: map[string]Embedding{
			"vec": makeDenseEmbedding(t, 1, 2, 3),
		},
		Limit: 10,
	}

	q, err := req.ToRemoteIndexQuery()
	require.NoError(t, err)
	require.Len(t, q.Embeddings, 1)
	assert.Nil(t, q.FullTextSearch, "embeddings-only queries should stay vector-only")
}

func TestQueryRequestToRemoteIndexQuery_PreservesExplicitHybridSearch(t *testing.T) {
	ft := []byte(`{"match": "hello"}`)
	req := &QueryRequest{
		Embeddings: map[string]Embedding{
			"vec": makeDenseEmbedding(t, 1, 2, 3),
		},
		FullTextSearch: ft,
		Limit:          10,
	}

	q, err := req.ToRemoteIndexQuery()
	require.NoError(t, err)
	require.Len(t, q.Embeddings, 1)
	require.NotNil(t, q.FullTextSearch, "explicit full-text search should still be parsed")

	rendered, err := jsonpkg.Marshal(q.FullTextSearch)
	require.NoError(t, err)
	assert.Contains(t, string(rendered), "match")
}

func TestQueryRequestToRemoteIndexQuery_InjectsMatchAllForLimitOnly(t *testing.T) {
	req := &QueryRequest{Limit: 10}

	q, err := req.ToRemoteIndexQuery()
	require.NoError(t, err)
	require.NotNil(t, q.FullTextSearch, "limit-only queries should still inject match_all")

	rendered, err := jsonpkg.Marshal(q.FullTextSearch)
	require.NoError(t, err)
	assert.Contains(t, string(rendered), "match_all")
}
