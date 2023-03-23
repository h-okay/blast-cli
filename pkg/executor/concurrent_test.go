package executor

import (
	"context"
	"testing"

	"github.com/datablast-analytics/blast/pkg/pipeline"
	"github.com/datablast-analytics/blast/pkg/scheduler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
)

func TestConcurrent_Start(t *testing.T) {
	t.Parallel()

	t11 := &pipeline.Task{
		Name: "task11",
		Type: "test",
	}

	t21 := &pipeline.Task{
		Name: "task21",
		Type: "test",
	}

	t12 := &pipeline.Task{
		Name:      "task12",
		Type:      "test",
		DependsOn: []string{"task11"},
	}

	t22 := &pipeline.Task{
		Name:      "task22",
		Type:      "test",
		DependsOn: []string{"task21"},
	}

	t3 := &pipeline.Task{
		Name:      "task3",
		Type:      "test",
		DependsOn: []string{"task12", "task22"},
	}

	p := &pipeline.Pipeline{
		Tasks: []*pipeline.Task{t11, t21, t12, t22, t3},
	}

	mockOperator := new(mockOperator)
	mockOperator.On("RunTask", mock.Anything, p, t11).
		Return(nil).
		Once()

	mockOperator.On("RunTask", mock.Anything, p, t21).
		Return(nil).
		Once()

	mockOperator.On("RunTask", mock.Anything, p, t12).
		Return(nil).
		Once()

	mockOperator.On("RunTask", mock.Anything, p, t22).
		Return(nil).
		Once()

	mockOperator.On("RunTask", mock.Anything, p, t3).
		Return(nil).
		Once()

	logger := zap.NewNop().Sugar()
	s := scheduler.NewScheduler(logger, p)

	ex := NewConcurrent(logger, map[string]Operator{"test": mockOperator}, 8)
	ex.Start(s.WorkQueue, s.Results)

	results := s.Run(context.Background())
	assert.Len(t, results, len(p.Tasks))

	mockOperator.AssertExpectations(t)
}
