package bigquery

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"
)

type DB struct {
	client *bigquery.Client
}

func NewDB(c *Config) *DB {
	client, err := bigquery.NewClient(
		context.Background(),
		c.ProjectID,
		option.WithCredentialsFile(c.CredentialsFilePath),
	)
	if err != nil {
		panic(fmt.Sprintf("cannot create bigquery client: %v", err))
	}

	if c.Location != "" {
		client.Location = c.Location
	}

	return &DB{
		client: client,
	}
}

func (d DB) IsValid(ctx context.Context, query string) (bool, error) {
	q := d.client.Query(query)
	q.DryRun = true

	job, err := q.Run(ctx)
	if err != nil {
		return false, err
	}

	status := job.LastStatus()
	if err := status.Err(); err != nil {
		return false, err
	}

	return true, nil
}
