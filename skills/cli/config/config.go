package config

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env/v2"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/providers/structs"
	"github.com/knadh/koanf/v2"
)

const (
	DefaultServerUrl = "http://localhost:18080"
	EnvPREFIX        = "SHS_CLI__"
)

func Load(path string) (*Config, error) {
	k := koanf.New(".")

	err := k.Load(file.Provider(path), yaml.Parser())
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("could not load %s, %w", path, err)
	}

	// SHS_CLI__SERVERS__LOCAL__URL -> servers.local.url
	// Double underscore (__) separates nesting levels.
	if err := k.Load(env.Provider(".", env.Opt{
		Prefix: EnvPREFIX,
		TransformFunc: func(k, v string) (string, any) {
			key := strings.ToLower(strings.TrimPrefix(k, EnvPREFIX))
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

	if len(k.MapKeys("servers")) == 0 {
		if err := setDefault(k); err != nil {
			return nil, err
		}
		return &cfg, k.Unmarshal("", &cfg)
	}
	return &cfg, nil
}

func setDefault(k *koanf.Koanf) error {
	defaultValues := Config{
		Servers: map[string]Server{
			"local": {
				Default:   true,
				URL:       DefaultServerUrl,
				VerifySSL: false,
			},
		},
	}
	err := k.Load(structs.Provider(defaultValues, "koanf"), nil)
	if err != nil {
		return err
	}
	return nil
}

type Config struct {
	Servers map[string]Server `koanf:"servers" yaml:"servers"`
}

type Server struct {
	Default       bool   `koanf:"default" yaml:"default"`
	URL           string `koanf:"url" yaml:"url"`
	VerifySSL     bool   `koanf:"verify_ssl" yaml:"verify_ssl"`
	Auth          *Auth  `koanf:"auth" yaml:"auth,omitempty"`
	EMRClusterARN string `koanf:"emr_cluster_arn" yaml:"emr_cluster_arn,omitempty"`
}

type Auth struct {
	Username string `koanf:"username" yaml:"username,omitempty"`
	Password string `koanf:"password" yaml:"password,omitempty"`
	Token    string `koanf:"token" yaml:"token,omitempty"`
}
