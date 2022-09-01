package bigquery

import (
	"context"

	"github.com/datablast-analytics/blast-cli/pkg/pipeline"
	"github.com/datablast-analytics/blast-cli/pkg/query"
	"github.com/pkg/errors"
)

type querier interface {
	RunQueryWithoutResult(ctx context.Context, q *query.Query) error
}

type queryExtractor interface {
	ExtractQueriesFromFile(filepath string) ([]*query.Query, error)
}

type BasicOperator struct {
	client    querier
	extractor queryExtractor
}

func NewBasicOperatorFromGlobals(extractor queryExtractor) (*BasicOperator, error) {
	config, err := LoadConfigFromEnv()
	if err != nil || !config.IsValid() {
		return nil, errors.New("failed to setup bigquery connection, please set the BIGQUERY_CREDENTIALS_FILE and BIGQUERY_PROJECT environment variables.")
	}

	bq, err := NewDB(config)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect to bigquery")
	}

	return NewBasicOperator(bq, extractor), nil
}

func NewBasicOperator(client *DB, extractor queryExtractor) *BasicOperator {
	return &BasicOperator{
		client:    client,
		extractor: extractor,
	}
}

func (o BasicOperator) RunTask(ctx context.Context, p *pipeline.Pipeline, t *pipeline.Task) error {
	queries, err := o.extractor.ExtractQueriesFromFile(t.ExecutableFile.Path)
	if err != nil {
		return errors.Wrap(err, "cannot extract queries from the task file")
	}

	if len(queries) == 0 {
		return nil
	}

	return o.client.RunQueryWithoutResult(ctx, queries[0])
}
