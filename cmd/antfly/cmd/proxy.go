package cmd

import (
	"fmt"
	"os"

	antflyproxy "github.com/antflydb/antfly/pkg/proxy"
	"github.com/spf13/cobra"
)

var proxyListenAddr string

var proxyCmd = &cobra.Command{
	Use:   "proxy",
	Short: "Run the Antfly-aware proxy gateway",
	Long: `Run the Antfly-aware proxy gateway for routing requests between
stateful and serverless backends.

The proxy is primarily configured through environment variables injected by the
operator, including:
  - ANTFLY_PROXY_ROUTES_JSON or ANTFLY_PROXY_ROUTES_FILE
  - ANTFLY_PROXY_REQUIRE_AUTH
  - ANTFLY_PROXY_BEARER_TOKENS_JSON or ANTFLY_PROXY_BEARER_TOKENS_FILE
  - ANTFLY_PROXY_PUBLIC_ADDR`,
	RunE: runProxy,
}

func init() {
	rootCmd.AddCommand(proxyCmd)
	proxyCmd.Flags().StringVar(&proxyListenAddr, "listen", "", "listen address override (defaults to ANTFLY_PROXY_PUBLIC_ADDR or :8080)")
}

func runProxy(cmd *cobra.Command, args []string) error {
	server, err := antflyproxy.NewServerFromEnv(func(key string) string {
		if key == "ANTFLY_PROXY_PUBLIC_ADDR" && proxyListenAddr != "" {
			return proxyListenAddr
		}
		return os.Getenv(key)
	})
	if err != nil {
		return fmt.Errorf("load proxy config: %w", err)
	}
	return server.ListenAndServe()
}
