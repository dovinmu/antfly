package proxy

import "testing"

func TestParseRoutesJSON(t *testing.T) {
	routes, err := ParseRoutesJSON(`[{"tenant":"t1","table":"docs","serving_namespace":"docs-serving","preferred_backend":"serverless","allow_stateful":true,"allow_serverless":true,"stateful_url":"http://stateful","serverless_url":"http://serverless"}]`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(routes) != 1 {
		t.Fatalf("got %d routes", len(routes))
	}
	if routes[0].PreferredBackend != BackendServerless {
		t.Fatalf("got backend %q", routes[0].PreferredBackend)
	}
	if routes[0].Table != "docs" || routes[0].Namespace != "docs-serving" {
		t.Fatalf("unexpected route mapping: %+v", routes[0])
	}
}
