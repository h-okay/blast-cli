package config

import (
	"bufio"
	errors "errors"
	"fmt"
	fs2 "io/fs"
	"os"
	"path"
	"strings"

	path2 "github.com/datablast-analytics/blast/pkg/path"
	"github.com/spf13/afero"
	"golang.org/x/oauth2/google"
)

type Manager struct{}

type BigQueryConnection struct {
	Name               string `yaml:"name"`
	ServiceAccountJSON string `yaml:"service_account_json"`
	ServiceAccountFile string `yaml:"service_account_file"`
	ProjectID          string `yaml:"project_id"`
	rawCredentials     *google.Credentials
}

func (c *BigQueryConnection) SetCredentials(cred *google.Credentials) {
	c.rawCredentials = cred
}

func (c *BigQueryConnection) GetCredentials() *google.Credentials {
	return c.rawCredentials
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

	DefaultEnvironmentName string `yaml:"default_environment"`
	DefaultEnvironment     *Environment
	Environments           map[string]Environment `yaml:"environments"`
}

func (c *Config) Persist() error {
	return path2.WriteYaml(c.fs, c.path, c)
}

func (c *Config) PersistToFs(fs afero.Fs) error {
	return path2.WriteYaml(fs, c.path, c)
}

func LoadFromFile(fs afero.Fs, path string) (*Config, error) {
	var config Config

	err := path2.ReadYaml(fs, path, &config)
	if err != nil {
		return nil, err
	}

	config.fs = fs
	config.path = path

	e, ok := config.Environments[config.DefaultEnvironmentName]
	if !ok {
		return nil, errors.New("default environment not found")
	}

	config.DefaultEnvironment = &e
	return &config, nil
}

func LoadOrCreate(fs afero.Fs, path string) (*Config, error) {
	config, err := LoadFromFile(fs, path)
	if err != nil && !errors.Is(err, fs2.ErrNotExist) {
		return nil, err
	}

	if err == nil {
		return config, ensureConfigIsInGitignore(fs, path)
	}

	config = &Config{
		fs:   fs,
		path: path,

		DefaultEnvironmentName: "default",
		Environments: map[string]Environment{
			"default": {
				Connections: Connections{},
			},
		},
	}

	err = config.Persist()
	if err != nil {
		return nil, fmt.Errorf("failed to persist config: %w", err)
	}

	return config, ensureConfigIsInGitignore(fs, path)
}

func ensureConfigIsInGitignore(fs afero.Fs, filePath string) (err error) {
	// Check if .gitignore file exists in the root of the repository
	gitignorePath := path.Join(path.Dir(filePath), ".gitignore")
	exists, err := afero.Exists(fs, gitignorePath)
	if err != nil {
		return err
	}

	fileNameToIgnore := path.Base(filePath)
	if !exists {
		// Create a new .gitignore file if it doesn't exist
		if err = afero.WriteFile(fs, gitignorePath, []byte(fileNameToIgnore), 0o644); err != nil {
			return err
		}
		return nil
	}

	file, err := fs.OpenFile(gitignorePath, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	defer func(open afero.File) {
		tempErr := open.Close()
		if tempErr != nil {
			err = errors.Join(err, fmt.Errorf("failed to close file: %w", tempErr))
		}
	}(file)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if strings.TrimSpace(scanner.Text()) == fileNameToIgnore {
			return nil
		}
	}

	_, err = file.Write([]byte("\n" + fileNameToIgnore))
	return err
}
