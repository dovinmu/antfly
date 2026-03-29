// Package proxy contains the Antfly-aware public gateway seam.
//
// The first responsibility of this package is to provide a stable place for:
//   - backend routing between stateful and serverless products
//   - tenant and namespace-aware request policy
//   - request-level freshness/consistency controls
//   - authn/authz integration
//
// The proxy should stay out of query execution and storage coordination.
package proxy
