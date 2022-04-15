package bigquery

import "github.com/kelseyhightower/envconfig"

type Config struct {
	ProjectID           string `envconfig:"BIGQUERY_PROJECT"`
	CredentialsFilePath string `envconfig:"BIGQUERY_CREDENTIALS_FILE"`
	Location            string `envconfig:"BIGQUERY_LOCATION"`
}

func (c Config) IsValid() bool {
	return c.ProjectID != "" && c.CredentialsFilePath != ""
}

func LoadConfigFromEnv() (*Config, error) {
	var cfg Config
	err := envconfig.Process("", &cfg)
	if err != nil {
		return nil, err
	}

	return &cfg, nil
}
