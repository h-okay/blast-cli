package executor

import (
	"context"

	"github.com/datablast-analytics/blast-cli/pkg/pipeline"
)

type NoOpOperator struct{}

func (e NoOpOperator) RunTask(ctx context.Context, p *pipeline.Pipeline, t *pipeline.Task) error {
	return nil
}
