package sim

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/ajroetker/go-highway/hwy/contrib/vec"
	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/store/db"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/antflydb/antfly/src/tablemgr"
	"github.com/stretchr/testify/require"
)

func TestHarness_CosineEmbeddingsIndexAcceptsNormalizedVectorsAcrossInternalSplits(t *testing.T) {
	const (
		indexName  = "vec"
		numVectors = 20_000
		dims       = 32
		batchSize  = 250
	)

	h, err := NewHarness(HarnessConfig{
		BaseDir:           t.TempDir(),
		Start:             time.Unix(1_700_000_000, 0).UTC(),
		MetadataIDs:       []types.ID{100},
		StoreIDs:          []types.ID{1},
		ReplicationFactor: 1,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, h.Close())
	})

	tableSchema := &schema.TableSchema{
		DefaultType: "default",
		DocumentSchemas: map[string]schema.DocumentSchema{
			"default": {
				Schema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"content": map[string]any{"type": "string"},
					},
				},
			},
		},
	}
	indexCfg := *indexes.NewEmbeddingsConfig(indexName, indexes.EmbeddingsIndexConfig{
		Dimension:      dims,
		Field:          "content",
		DistanceMetric: indexes.DistanceMetricCosine,
	})
	table, err := h.CreateTable("docs", tablemgr.TableConfig{
		NumShards: 1,
		StartID:   0x1000,
		Schema:    tableSchema,
		Indexes: map[string]indexes.IndexConfig{
			indexName: indexCfg,
		},
	})
	require.NoError(t, err)

	var shardID types.ID
	for id := range table.Shards {
		shardID = id
		break
	}
	require.NotZero(t, shardID)
	require.NoError(t, h.StartShardOnAllStores(shardID))

	leaderID, err := h.WaitForLeader(shardID, 20*time.Second)
	require.NoError(t, err)
	status, err := h.tableManager.GetStoreStatus(context.Background(), leaderID)
	require.NoError(t, err)

	rng := rand.New(rand.NewPCG(42, 1024))
	err = h.runWithProgress(20*time.Minute, func() error {
		for start := 0; start < numVectors; start += batchSize {
			end := min(start+batchSize, numVectors)
			writes := make([][2][]byte, 0, end-start)
			for i := start; i < end; i++ {
				embedding := make([]float32, dims)
				for j := range embedding {
					embedding[j] = rng.Float32()*2 - 1
				}
				vec.NormalizeFloat32(embedding)

				docBytes, err := json.Marshal(map[string]any{
					"content": fmt.Sprintf("document %d", i),
					"_embeddings": map[string]any{
						indexName: embedding,
					},
				})
				if err != nil {
					return fmt.Errorf("marshal doc %d: %w", i, err)
				}
				key := []byte(fmt.Sprintf("doc/%05d", i))
				writes = append(writes, [2][]byte{key, docBytes})
			}
			if err := status.StoreClient.Batch(
				context.Background(),
				shardID,
				writes,
				nil,
				nil,
				db.Op_SyncLevelEmbeddings,
			); err != nil && !errors.Is(err, db.ErrPartialSuccess) {
				return fmt.Errorf("write batch starting at %d: %w", start, err)
			}
		}
		return nil
	})
	require.NoError(t, err)

	require.NoError(t, h.WaitFor(20*time.Minute, func() error {
		if err := h.RefreshStatuses(context.Background()); err != nil {
			return err
		}
		shardStatus, err := h.GetShardStatus(shardID)
		if err != nil {
			return err
		}
		if shardStatus.ShardStats == nil {
			return fmt.Errorf("shard stats not available yet")
		}
		stats, ok := shardStatus.ShardStats.Indexes[indexName]
		if !ok {
			return fmt.Errorf("index stats for %s not available yet", indexName)
		}
		indexStats, err := stats.AsEmbeddingsIndexStats()
		if err != nil {
			return err
		}
		if indexStats.TotalIndexed < uint64(numVectors) {
			return fmt.Errorf("indexed %d/%d vectors", indexStats.TotalIndexed, numVectors)
		}
		return nil
	}))

	lookup, err := h.LookupFromStore(leaderID, shardID, []string{"doc/00000", "doc/10000", "doc/19999"})
	require.NoError(t, err)
	require.Len(t, lookup, 3)
}
