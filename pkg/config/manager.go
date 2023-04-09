package config

import (
	"fmt"

	path2 "github.com/datablast-analytics/blast/pkg/path"
	"github.com/spf13/afero"
	"gopkg.in/yaml.v3"
)

type Manager struct{}

type BigQueryConnection struct {
	ServiceAccountJSON string
	ServiceAccountFile string
	ProjectID          string
}

type SnowflakeConnection struct {
	Account   string
	Username  string
	Password  string
	Region    string
	Role      string
	Database  string
	Schema    string
	Warehouse string
}

type Connections struct {
	BigQuery  map[string]BigQueryConnection
	Snowflake map[string]SnowflakeConnection
}

func getStringValue(values map[string]interface{}, key string) string {
	if values == nil {
		return ""
	}

	val, ok := values[key]
	if !ok {
		return ""
	}

	strVal, ok := val.(string)
	if !ok {
		return ""
	}

	return strVal
}

func (c *Connections) UnmarshalYAML(value *yaml.Node) error {
	var raw map[string]map[string]interface{}
	err := value.Decode(&raw)
	if err != nil {
		return err
	}

	connections := Connections{
		BigQuery:  map[string]BigQueryConnection{},
		Snowflake: map[string]SnowflakeConnection{},
	}

	for connName, rawVal := range raw {
		typeName, ok := rawVal["type"].(string)
		if !ok {
			return fmt.Errorf("connection type not specified")
		}

		switch typeName {
		case "bigquery":
			conn := BigQueryConnection{
				ServiceAccountJSON: getStringValue(rawVal, "service_account_json"),
				ServiceAccountFile: getStringValue(rawVal, "service_account_file"),
				ProjectID:          getStringValue(rawVal, "project_id"),
			}
			connections.BigQuery[connName] = conn
		case "snowflake":
			conn := SnowflakeConnection{
				Account:   getStringValue(rawVal, "account"),
				Username:  getStringValue(rawVal, "username"),
				Password:  getStringValue(rawVal, "password"),
				Region:    getStringValue(rawVal, "region"),
				Role:      getStringValue(rawVal, "role"),
				Database:  getStringValue(rawVal, "database"),
				Schema:    getStringValue(rawVal, "schema"),
				Warehouse: getStringValue(rawVal, "warehouse"),
			}
			connections.Snowflake[connName] = conn
		default:
			return fmt.Errorf("unknown connection type: %s", typeName)
		}
	}

	*c = connections
	return err
}

type Environment struct {
	Connections Connections `yaml:"connections"`
}

type Config struct {
	Environments map[string]Environment `yaml:"environments"`
}

func LoadFromFile(fs afero.Fs, path string) (*Config, error) {
	var config Config

	err := path2.ReadYaml(fs, path, &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}
