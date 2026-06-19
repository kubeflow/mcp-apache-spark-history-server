package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
	// EnvConfig points at an explicit config file.
	EnvConfig = "SHS_CLI__CONFIG"
	// DefaultConfigName is looked up in the cwd and the config home.
	DefaultConfigName = "config.yaml"
	// AppConfigDir is the per-user config subdir under the XDG config home,
	// shared with the MCP server (~/.config/spark-mcp/config.yaml).
	AppConfigDir = "spark-mcp"
)

// userConfigPath returns $XDG_CONFIG_HOME (if set) or ~/.config, joined with
// the app dir and config name. Returns "" if the home dir is unknown.
func userConfigPath() string {
	base := os.Getenv("XDG_CONFIG_HOME")
	if base == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return ""
		}
		base = filepath.Join(home, ".config")
	}
	return filepath.Join(base, AppConfigDir, DefaultConfigName)
}

// resolvePath picks the config file, highest precedence first: the --config
// flag, then SHS_CLI__CONFIG, then ./config.yaml, then
// ~/.config/spark-mcp/config.yaml. It returns (path, explicit); an explicit
// path is returned without an existence check so a missing file fails fast,
// and path is "" when nothing is found (fall back to defaults).
func resolvePath(flagPath string) (path string, explicit bool) {
	if flagPath != "" {
		return flagPath, true
	}
	if v := os.Getenv(EnvConfig); v != "" {
		return v, true
	}
	if _, err := os.Stat(DefaultConfigName); err == nil {
		return DefaultConfigName, false
	}
	if userPath := userConfigPath(); userPath != "" {
		if _, err := os.Stat(userPath); err == nil {
			return userPath, false
		}
	}
	return "", false
}

func Load(flagPath string) (*Config, error) {
	k := koanf.New(".")

	path, explicit := resolvePath(flagPath)
	if path != "" {
		err := k.Load(file.Provider(path), yaml.Parser())
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				// Explicit but missing -> fatal; discovered -> use defaults.
				if explicit {
					return nil, fmt.Errorf(
						"config file not found: %s (set via --config or %s)", path, EnvConfig)
				}
			} else {
				return nil, fmt.Errorf("could not load %s, %w", path, err)
			}
		}
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
