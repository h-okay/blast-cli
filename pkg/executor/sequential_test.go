package executor

import (
	"context"
	"testing"

	"github.com/datablast-analytics/blast/pkg/pipeline"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockOperator struct {
	mock.Mock
}

func (d *mockOperator) RunTask(ctx context.Context, p *pipeline.Pipeline, t *pipeline.Task) error {
	args := d.Called(ctx, p, t)
	return args.Error(0)
}

func TestLocal_RunSingleTask(t *testing.T) {
	t.Parallel()

	p := &pipeline.Pipeline{}
	task := &pipeline.Task{
		Name: "task1",
		Type: "test",
	}

	t.Run("simple task is executed successfully", func(t *testing.T) {
		t.Parallel()

		mockOperator := new(mockOperator)
		mockOperator.On("RunTask", mock.Anything, p, task).
			Return(nil)

		l := Sequential{
			TaskTypeMap: map[string]Operator{
				"test": mockOperator,
			},
		}

		err := l.RunSingleTask(context.Background(), p, task)

		assert.NoError(t, err)
		mockOperator.AssertExpectations(t)
	})

	t.Run("missing task is rejected", func(t *testing.T) {
		t.Parallel()

		mockOperator := new(mockOperator)

		l := Sequential{
			TaskTypeMap: map[string]Operator{
				"some-other-task": mockOperator,
			},
		}

		err := l.RunSingleTask(context.Background(), p, task)

		assert.Error(t, err)
		mockOperator.AssertExpectations(t)
	})

	t.Run("missing task is rejected", func(t *testing.T) {
		t.Parallel()

		mockOperator := new(mockOperator)
		mockOperator.On("RunTask", mock.Anything, p, task).
			Return(errors.New("some error occurred"))

		l := Sequential{
			TaskTypeMap: map[string]Operator{
				"test": mockOperator,
			},
		}

		err := l.RunSingleTask(context.Background(), p, task)

		assert.Error(t, err)
		mockOperator.AssertExpectations(t)
	})
}
