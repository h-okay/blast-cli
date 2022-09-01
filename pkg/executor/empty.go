package executor

import (
	"context"

	"github.com/datablast-analytics/blast-cli/pkg/pipeline"
)

type EmptyOperator struct{}

func (e EmptyOperator) RunTask(ctx context.Context, p *pipeline.Pipeline, t *pipeline.Task) error {
	return nil
}
