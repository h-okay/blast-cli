package executor

import (
	"context"

	"github.com/datablast-analytics/blast/pkg/scheduler"
)

type NoOpOperator struct{}

func (e NoOpOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	return nil
}
