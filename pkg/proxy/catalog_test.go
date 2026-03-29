package proxy

import (
	"context"
	"testing"
)

func TestChainedCatalogFallsBackToStaticRoutes(t *testing.T) {
	catalog := NewChainedCatalog(
		NewStaticCatalog(nil),
		NewStaticCatalog([]NamespaceRoute{
			{
				Tenant:          "t1",
				Table:           "docs",
				Namespace:       "docs-serving",
				AllowServerless: true,
				ServerlessURL:   "http://serverless",
			},
		}),
	)

	route, err := catalog.ResolveRoute(context.Background(), "t1", "docs")
	if err != nil {
		t.Fatalf("unexpected resolve error: %v", err)
	}
	if route.ServerlessURL != "http://serverless" {
		t.Fatalf("got route %+v", route)
	}
}
