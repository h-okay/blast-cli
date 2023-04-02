package bigquery

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
	"github.com/datablast-analytics/blast/pkg/query"
	"github.com/pkg/errors"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

var scopes = []string{
	bigquery.Scope,
	"https://www.googleapis.com/auth/cloud-platform",
	"https://www.googleapis.com/auth/drive",
}

type DB struct {
	client *bigquery.Client
}

func NewDB(c *Config) (*DB, error) {
	client, err := bigquery.NewClient(
		context.Background(),
		c.ProjectID,
		option.WithCredentialsFile(c.CredentialsFilePath),
		option.WithScopes(scopes...),
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create bigquery client")
	}

	if c.Location != "" {
		client.Location = c.Location
	}

	return &DB{
		client: client,
	}, nil
}

func (d DB) IsValid(ctx context.Context, query *query.Query) (bool, error) {
	q := d.client.Query(query.ToDryRunQuery())
	q.DryRun = true

	job, err := q.Run(ctx)
	if err != nil {
		return false, formatError(err)
	}

	status := job.LastStatus()
	if err := status.Err(); err != nil {
		return false, err
	}

	return true, nil
}

func (d DB) RunQueryWithoutResult(ctx context.Context, query *query.Query) error {
	q := d.client.Query(query.String())
	_, err := q.Read(ctx)
	if err != nil {
		return formatError(err)
	}

	return nil
}

func (d DB) Select(ctx context.Context, query *query.Query) ([][]interface{}, error) {
	q := d.client.Query(query.String())
	rows, err := q.Read(ctx)
	if err != nil {
		return nil, formatError(err)
	}

	result := make([][]interface{}, 0)
	for {
		var values []bigquery.Value
		err := rows.Next(&values)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, err
		}

		interfaces := make([]interface{}, len(values))
		for i, v := range values {
			interfaces[i] = v
		}

		result = append(result, interfaces)
	}

	return result, nil
}

func formatError(err error) error {
	var googleError *googleapi.Error
	if !errors.As(err, &googleError) {
		return err
	}

	if googleError.Code == 404 || googleError.Code == 400 {
		return fmt.Errorf("%s", googleError.Message)
	}

	return googleError
}
