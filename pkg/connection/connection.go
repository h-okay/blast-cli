package connection

import (
	"errors"

	"github.com/datablast-analytics/blast/pkg/bigquery"
)

type Manager struct {
	BigQuery map[string]*bigquery.DB
}

func (m *Manager) GetBqConnection(name string) (*bigquery.DB, error) {
	if m.BigQuery == nil {
		return nil, errors.New("no bigquery connections found")
	}

	db, ok := m.BigQuery[name]
	if !ok {
		return nil, errors.New("bigquery connection not found")
	}

	return db, nil
}
