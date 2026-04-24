package util

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/client"
	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/config"
)

const DefaultTimeout = 10 * time.Second

func NewSHSClient(configPath string, opts ...Option) (client.ClientWithResponsesInterface, error) {
	o := options{timeout: DefaultTimeout}
	for _, fn := range opts {
		fn(&o)
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		return nil, fmt.Errorf("loading config: %w", err)
	}

	var server config.Server
	var found bool
	if o.serverName != "" {
		server, found = cfg.Servers[o.serverName]
		if !found {
			return nil, fmt.Errorf("server %q not found in config", o.serverName)
		}
	} else {
		for _, s := range cfg.Servers {
			if s.Default {
				server = s
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("no default server in config")
		}
	}

	httpClient := &http.Client{Timeout: o.timeout}
	return client.NewClientWithResponses(server.URL+"/api/v1",
		client.WithHTTPClient(httpClient),
		client.WithRequestEditorFn(unescapeAppAttemptPath),
	)
}

type options struct {
	timeout    time.Duration
	serverName string
}

type Option func(*options)

func WithTimeout(d time.Duration) Option {
	return func(o *options) { o.timeout = d }
}

func WithServer(name string) Option {
	return func(o *options) { o.serverName = name }
}

// unescapeAppAttemptPath rewrites %2F back to / in the URL path so that
// YARN-style app IDs like "application_123/1" are routed correctly as
// /applications/{appId}/{attemptId}/... instead of /applications/{appId%2FattemptId}/...
func unescapeAppAttemptPath(_ context.Context, req *http.Request) error {
	if strings.Contains(req.URL.RawPath, "%2F") {
		req.URL.RawPath = strings.ReplaceAll(req.URL.RawPath, "%2F", "/")
		req.URL.Path = req.URL.RawPath
	}
	return nil
}
