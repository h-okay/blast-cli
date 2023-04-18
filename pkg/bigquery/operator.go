package bigquery

import (
	"context"

	"github.com/datablast-analytics/blast/pkg/pipeline"
	"github.com/datablast-analytics/blast/pkg/query"
	"github.com/datablast-analytics/blast/pkg/scheduler"
	"github.com/pkg/errors"
)

type materializer interface {
	Render(task *pipeline.Asset, query string) (string, error)
}

type queryExtractor interface {
	ExtractQueriesFromFile(filepath string) ([]*query.Query, error)
}

type connectionFetcher interface {
	GetBqConnection(name string) (DB, error)
}

type BasicOperator struct {
	connection   connectionFetcher
	extractor    queryExtractor
	materializer materializer
}

func NewBasicOperator(conn connectionFetcher, extractor queryExtractor, materializer materializer) *BasicOperator {
	return &BasicOperator{
		connection:   conn,
		extractor:    extractor,
		materializer: materializer,
	}
}

func (o BasicOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	return o.RunTask(ctx, ti.GetPipeline(), ti.GetAsset())
}

func (o BasicOperator) RunTask(ctx context.Context, p *pipeline.Pipeline, t *pipeline.Asset) error {
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

	conn, err := o.connection.GetBqConnection(p.GetConnectionNameForAsset(t))
	if err != nil {
		return err
	}

	return conn.RunQueryWithoutResult(ctx, q)
}

type testRunner interface {
	Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error
}

type ColumnCheckOperator struct {
	testRunners map[string]testRunner
}

func NewColumnCheckOperator(manager connectionFetcher) (*ColumnCheckOperator, error) {
	return &ColumnCheckOperator{
		testRunners: map[string]testRunner{
			"not_null":        &NotNullCheck{conn: manager},
			"unique":          &UniqueCheck{conn: manager},
			"positive":        &PositiveCheck{conn: manager},
			"accepted_values": &AcceptedValuesCheck{conn: manager},
		},
	}, nil
}

func (o ColumnCheckOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	test, ok := ti.(*scheduler.ColumnCheckInstance)
	if !ok {
		return errors.New("cannot run a non-column test instance")
	}

	executor, ok := o.testRunners[test.Check.Name]
	if !ok {
		return errors.New("there is no executor configured for the test type, test cannot be run: " + test.Check.Name)
	}

	return executor.Check(ctx, test)
}
