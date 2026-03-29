package proxy

import (
	"fmt"
	"strings"
)

const (
	ViewPublished = "published"
	ViewLatest    = "latest"
)

func NormalizePolicy(policy RequestPolicy) RequestPolicy {
	if strings.TrimSpace(policy.View) == "" {
		policy.View = ViewPublished
	}
	return policy
}

func ValidatePolicy(req RequestContext) error {
	req.Policy = NormalizePolicy(req.Policy)

	switch req.Policy.View {
	case ViewPublished, ViewLatest:
	default:
		return fmt.Errorf("unsupported view %q", req.Policy.View)
	}

	if req.Operation == OperationWrite {
		if req.Policy.View == ViewLatest {
			return fmt.Errorf("write requests cannot require latest published view")
		}
		if req.Policy.MaxLagRecords > 0 {
			return fmt.Errorf("write requests cannot set max_lag_records")
		}
	}

	if req.Policy.RequiredVersion != nil && req.Policy.View == ViewLatest {
		return fmt.Errorf("required_version cannot be combined with latest view")
	}

	return nil
}
