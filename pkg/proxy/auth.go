package proxy

import (
	"fmt"
	"net/http"
	"strings"
)

type Principal struct {
	Subject           string
	Tenant            string
	Admin             bool
	AllowedTables     []string
	AllowedNamespaces []string
	AllowedOperations []OperationKind
}

type Authenticator interface {
	Authenticate(r *http.Request) (*Principal, error)
}

type Authorizer interface {
	Authorize(principal *Principal, req RequestContext, route NamespaceRoute) error
}

type StaticBearerAuthenticator struct {
	Required bool
	Tokens   map[string]Principal
}

func (a StaticBearerAuthenticator) Authenticate(r *http.Request) (*Principal, error) {
	header := strings.TrimSpace(r.Header.Get("Authorization"))
	if header == "" {
		if a.Required {
			return nil, fmt.Errorf("missing authorization header")
		}
		return &Principal{Subject: "anonymous"}, nil
	}
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return nil, fmt.Errorf("unsupported authorization scheme")
	}
	token := strings.TrimSpace(strings.TrimPrefix(header, prefix))
	principal, ok := a.Tokens[token]
	if !ok {
		return nil, fmt.Errorf("invalid bearer token")
	}
	return &principal, nil
}

type TenantAuthorizer struct{}

func (TenantAuthorizer) Authorize(principal *Principal, req RequestContext, route NamespaceRoute) error {
	if principal == nil {
		return fmt.Errorf("missing principal")
	}
	if principal.Admin {
		return nil
	}
	if strings.TrimSpace(principal.Tenant) == "" {
		return fmt.Errorf("principal has no tenant scope")
	}
	if principal.Tenant != req.Tenant {
		return fmt.Errorf("principal tenant %q cannot access tenant %q", principal.Tenant, req.Tenant)
	}
	if route.Tenant != "" && route.Tenant != principal.Tenant {
		return fmt.Errorf("route tenant %q does not match principal tenant %q", route.Tenant, principal.Tenant)
	}
	resource := firstNonEmpty(req.Table, route.TableName(), req.Namespace)
	switch {
	case len(principal.AllowedTables) > 0:
		if !allowsResource(principal.AllowedTables, resource) {
			return fmt.Errorf("principal %q cannot access table %q", principal.Subject, resource)
		}
	case len(principal.AllowedNamespaces) > 0:
		if !allowsResource(principal.AllowedNamespaces, route.ServingNamespace()) {
			return fmt.Errorf("principal %q cannot access table %q", principal.Subject, resource)
		}
	}
	if !allowsOperation(principal.AllowedOperations, req.Operation) {
		return fmt.Errorf("principal %q cannot perform operation %q", principal.Subject, req.Operation)
	}
	return nil
}

func allowsResource(allowed []string, resource string) bool {
	if len(allowed) == 0 {
		return true
	}
	resource = strings.TrimSpace(resource)
	for _, candidate := range allowed {
		candidate = strings.TrimSpace(candidate)
		if candidate == "*" || candidate == resource {
			return true
		}
	}
	return false
}

func allowsOperation(allowed []OperationKind, operation OperationKind) bool {
	if len(allowed) == 0 {
		return true
	}
	for _, candidate := range allowed {
		if candidate == operation || candidate == "*" {
			return true
		}
	}
	return false
}
