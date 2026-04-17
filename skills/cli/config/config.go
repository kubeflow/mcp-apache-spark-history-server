package config

import (
	"fmt"
	"strings"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env/v2"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

func Load(path string) (*Config, error) {
	k := koanf.New(".")

	if err := k.Load(file.Provider(path), yaml.Parser()); err != nil {
		return nil, fmt.Errorf("reading config: %w", err)
	}

	// SHS_CLI__SERVERS__LOCAL__URL -> servers.local.url
	// Double underscore (__) separates nesting levels.
	if err := k.Load(env.Provider(".", env.Opt{
		Prefix: "SHS_CLI__",
		TransformFunc: func(k, v string) (string, any) {
			key := strings.ToLower(strings.TrimPrefix(k, "SHS_CLI__"))
			key = strings.ReplaceAll(key, "__", ".")
			return key, v
		},
	}), nil); err != nil {
		return nil, fmt.Errorf("loading env: %w", err)
	}

	var cfg Config
	if err := k.Unmarshal("", &cfg); err != nil {
		return nil, fmt.Errorf("unmarshalling config: %w", err)
	}
	return &cfg, nil
}

type Config struct {
	Servers map[string]Server `koanf:"servers"`
}

type Server struct {
	Default       bool   `koanf:"default"`
	URL           string `koanf:"url"`
	VerifySSL     *bool  `koanf:"verify_ssl"`
	Auth          *Auth  `koanf:"auth"`
	EMRClusterARN string `koanf:"emr_cluster_arn"`
}

type Auth struct {
	Username string `koanf:"username"`
	Password string `koanf:"password"`
	Token    string `koanf:"token"`
}
