package connection

import (
	"errors"

	"github.com/datablast-analytics/blast/pkg/bigquery"
	"github.com/datablast-analytics/blast/pkg/config"
)

type Manager struct {
	BigQuery map[string]*bigquery.DB
}

func (m *Manager) GetConnection(name string) (interface{}, error) {
	return m.GetBqConnection(name)
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

func (m *Manager) AddBqConnectionFromConfig(connection *config.GoogleCloudPlatformConnection) error {
	if m.BigQuery == nil {
		m.BigQuery = make(map[string]*bigquery.DB)
	}

	db, err := bigquery.NewDB(&bigquery.Config{
		ProjectID:           connection.ProjectID,
		CredentialsFilePath: connection.ServiceAccountFile,
		CredentialsJSON:     connection.ServiceAccountJSON,
		Credentials:         connection.GetCredentials(),
	})
	if err != nil {
		return err
	}

	m.BigQuery[connection.Name] = db

	return nil
}
