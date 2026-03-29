package proxy

import "testing"

func TestValidatePolicy(t *testing.T) {
	requiredVersion := new(uint64)
	*requiredVersion = 7

	tests := []struct {
		name string
		req  RequestContext
		ok   bool
	}{
		{
			name: "default published read is valid",
			req: RequestContext{
				Operation: OperationRead,
				Policy:    RequestPolicy{},
			},
			ok: true,
		},
		{
			name: "latest write is rejected",
			req: RequestContext{
				Operation: OperationWrite,
				Policy: RequestPolicy{
					View: ViewLatest,
				},
			},
		},
		{
			name: "required version with latest is rejected",
			req: RequestContext{
				Operation: OperationRead,
				Policy: RequestPolicy{
					View:            ViewLatest,
					RequiredVersion: requiredVersion,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidatePolicy(tt.req)
			if tt.ok && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !tt.ok && err == nil {
				t.Fatal("expected error")
			}
		})
	}
}
