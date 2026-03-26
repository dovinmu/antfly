package memoryaf

import (
	"context"

	"github.com/antflydb/antfly/pkg/client"
)

// Client is the minimal Antfly client interface required by memoryaf.
// Colony implements this via its ClusterManager; the desktop app can
// implement it with a direct HTTP client to a local Antfly instance.
type Client interface {
	CreateTable(ctx context.Context, tableName string, config *client.CreateTableRequest) error
	Batch(ctx context.Context, tableID string, batchRequest client.BatchRequest) (*client.BatchResult, error)
	QueryWithBody(ctx context.Context, requestBody []byte) ([]byte, error)
}
