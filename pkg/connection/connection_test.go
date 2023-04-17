package connection

import (
	"testing"

	"github.com/datablast-analytics/blast/pkg/bigquery"
	"github.com/stretchr/testify/assert"
)

func TestManager_GetBqConnection(t *testing.T) {
	t.Parallel()

	existingDB := new(bigquery.DB)
	m := Manager{
		BigQuery: map[string]*bigquery.DB{
			"another":  new(bigquery.DB),
			"existing": existingDB,
		},
	}

	tests := []struct {
		name           string
		connectionName string
		want           *bigquery.DB
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
