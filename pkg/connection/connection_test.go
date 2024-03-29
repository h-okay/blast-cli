package connection

import (
	"testing"

	"github.com/datablast-analytics/blast/pkg/bigquery"
	"github.com/datablast-analytics/blast/pkg/config"
	"github.com/stretchr/testify/assert"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

func TestManager_GetBqConnection(t *testing.T) {
	t.Parallel()

	existingDB := new(bigquery.Client)
	m := Manager{
		BigQuery: map[string]*bigquery.Client{
			"another":  new(bigquery.Client),
			"existing": existingDB,
		},
	}

	tests := []struct {
		name           string
		connectionName string
		want           bigquery.DB
		wantErr        assert.ErrorAssertionFunc
	}{
		{
			name:           "should return error when no connections are found",
			connectionName: "non-existing",
			wantErr:        assert.Error,
		},
		{
			name:           "should find the correct connection",
			connectionName: "existing",
			want:           existingDB,
			wantErr:        assert.NoError,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := m.GetBqConnection(tt.connectionName)
			tt.wantErr(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestManager_AddBqConnectionFromConfig(t *testing.T) {
	t.Parallel()

	m := Manager{}

	res, err := m.GetBqConnection("test")
	assert.Error(t, err)
	assert.Nil(t, res)

	connection := &config.GoogleCloudPlatformConnection{
		Name:      "test",
		ProjectID: "test",
	}
	connection.SetCredentials(&google.Credentials{
		ProjectID: "some-project-id",
		TokenSource: oauth2.StaticTokenSource(&oauth2.Token{
			AccessToken: "some-token",
		}),
	})

	err = m.AddBqConnectionFromConfig(connection)
	assert.NoError(t, err)

	res, err = m.GetBqConnection("test")
	assert.NoError(t, err)
	assert.NotNil(t, res)
}
