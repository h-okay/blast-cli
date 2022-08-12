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
