package config

import (
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
)

func TestLoadFromFile(t *testing.T) {
	t.Parallel()

	type args struct {
		path string
	}
	tests := []struct {
		name    string
		args    args
		want    *Config
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "missing path should error",
			args: args{
				path: "testdata/some/path/that/doesnt/exist",
			},
			wantErr: assert.Error,
		},
		{
			name: "read simple connection",
			args: args{
				path: "testdata/simple.yml",
			},
			want: &Config{
				Environments: map[string]Environment{
					"dev": {
						Connections: Connections{
							BigQuery: map[string]BigQueryConnection{
								"conn1": {
									ServiceAccountJSON: "{\"key1\": \"value1\"}",
									ServiceAccountFile: "/path/to/service_account.json",
									ProjectID:          "my-project",
								},
							},
							Snowflake: map[string]SnowflakeConnection{
								"conn2": {
									Username:  "user",
									Password:  "pass",
									Account:   "account",
									Region:    "region",
									Role:      "role",
									Database:  "db",
									Schema:    "schema",
									Warehouse: "wh",
								},
							},
						},
					},
					"prod": {
						Connections: Connections{
							BigQuery: map[string]BigQueryConnection{
								"conn1": {
									ServiceAccountFile: "/path/to/service_account.json",
									ProjectID:          "my-project",
								},
							},
							Snowflake: map[string]SnowflakeConnection{},
						},
					},
				},
			},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fs := afero.NewReadOnlyFs(afero.NewOsFs())
			got, err := LoadFromFile(fs, tt.args.path)

			tt.wantErr(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
