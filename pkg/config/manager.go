package config

import (
	"errors"
	fs2 "io/fs"

	path2 "github.com/datablast-analytics/blast/pkg/path"
	"github.com/spf13/afero"
)

type Manager struct{}

type BigQueryConnection struct {
	Name               string `yaml:"name"`
	ServiceAccountJSON string `yaml:"service_account_json"`
	ServiceAccountFile string `yaml:"service_account_file"`
	ProjectID          string `yaml:"project_id"`
}

type SnowflakeConnection struct {
	Name      string `yaml:"name"`
	Account   string `yaml:"account"`
	Username  string `yaml:"username"`
	Password  string `yaml:"password"`
	Region    string `yaml:"region"`
	Role      string `yaml:"role"`
	Database  string `yaml:"database"`
	Schema    string `yaml:"schema"`
	Warehouse string `yaml:"warehouse"`
}

type Connections struct {
	BigQuery  []BigQueryConnection
	Snowflake []SnowflakeConnection
}

type Environment struct {
	Connections Connections `yaml:"connections"`
}

type Config struct {
	fs   afero.Fs
	path string

	Environments map[string]Environment `yaml:"environments"`
}

func (c *Config) Persist() error {
	return path2.WriteYaml(c.fs, c.path, c)
}

func LoadFromFile(fs afero.Fs, path string) (*Config, error) {
	var config Config

	err := path2.ReadYaml(fs, path, &config)
	if err != nil {
		return nil, err
	}

	config.fs = fs
	config.path = path

	return &config, nil
}

func LoadOrCreate(fs afero.Fs, path string) (*Config, error) {
	config, err := LoadFromFile(fs, path)
	if err != nil && !errors.Is(err, fs2.ErrNotExist) {
		return nil, err
	}

	if err == nil {
		return config, nil
	}

	config = &Config{
		fs:   fs,
		path: path,

		Environments: map[string]Environment{
			"default": {
				Connections: Connections{},
			},
		},
	}

	err = config.Persist()
	if err != nil {
		return nil, err
	}

	return config, nil
}
