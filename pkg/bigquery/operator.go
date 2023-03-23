package bigquery

import (
	"context"

	"github.com/datablast-analytics/blast/pkg/pipeline"
	"github.com/datablast-analytics/blast/pkg/query"
	"github.com/pkg/errors"
)

type querier interface {
	RunQueryWithoutResult(ctx context.Context, q *query.Query) error
}

type materializer interface {
	Render(task *pipeline.Task, query string) (string, error)
}

type queryExtractor interface {
	ExtractQueriesFromFile(filepath string) ([]*query.Query, error)
}

type BasicOperator struct {
	client       querier
	extractor    queryExtractor
	materializer materializer
}

func NewBasicOperatorFromGlobals(extractor queryExtractor, materializer materializer) (*BasicOperator, error) {
	config, err := LoadConfigFromEnv()
	if err != nil || !config.IsValid() {
		return nil, errors.New("failed to setup bigquery connection, please set the BIGQUERY_CREDENTIALS_FILE and BIGQUERY_PROJECT environment variables.")
	}

	bq, err := NewDB(config)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect to bigquery")
	}

	return NewBasicOperator(bq, extractor, materializer), nil
}

func NewBasicOperator(client *DB, extractor queryExtractor, materializer materializer) *BasicOperator {
	return &BasicOperator{
		client:       client,
		extractor:    extractor,
		materializer: materializer,
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

	if len(queries) > 1 && t.Materialization.Type != pipeline.MaterializationTypeNone {
		return errors.New("cannot enable materialization for tasks with multiple queries")
	}

	q := queries[0]
	materialized, err := o.materializer.Render(t, q.String())
	if err != nil {
		return err
	}

	q.Query = materialized
	return o.client.RunQueryWithoutResult(ctx, q)
}
