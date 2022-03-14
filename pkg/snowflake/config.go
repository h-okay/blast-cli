package snowflake

import (
	"github.com/kelseyhightower/envconfig"
	"github.com/snowflakedb/gosnowflake"
)

type Config struct {
	Account  string `envconfig:"SNOWFLAKE_ACCOUNT"`
	Username string `envconfig:"SNOWFLAKE_USERNAME"`
	Password string `envconfig:"SNOWFLAKE_PASSWORD"`
	Region   string `envconfig:"SNOWFLAKE_REGION"`
}

func (c Config) DSN() (string, error) {
	snowflakeConfig := gosnowflake.Config{
		Account:  c.Account,
		User:     c.Username,
		Password: c.Password,
		Region:   c.Region,
	}

	return gosnowflake.DSN(&snowflakeConfig)
}

func LoadConfigFromEnv() (*Config, error) {
	var cfg Config
	err := envconfig.Process("", &cfg)
	if err != nil {
		return nil, err
	}

	return &cfg, nil
}
