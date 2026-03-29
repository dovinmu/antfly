package proxy

import "net/http"

type Server struct {
	Addr    string
	Gateway *Gateway
}

func NewServerFromEnv(getenv func(string) string) (*Server, error) {
	cfg, err := LoadGatewayEnvConfig(getenv)
	if err != nil {
		return nil, err
	}
	gateway, err := NewGatewayFromEnv(getenv)
	if err != nil {
		return nil, err
	}
	return &Server{
		Addr:    cfg.PublicAddr,
		Gateway: gateway,
	}, nil
}

func (s *Server) Handler() http.Handler {
	return s.Gateway
}

func (s *Server) ListenAndServe() error {
	return http.ListenAndServe(s.Addr, s.Handler())
}
