package executor

import (
	"context"
	"testing"

	"github.com/datablast-analytics/blast/pkg/pipeline"
	"github.com/datablast-analytics/blast/pkg/scheduler"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockOperator struct {
	mock.Mock
}

func (d *mockOperator) RunTask(ctx context.Context, p *pipeline.Pipeline, t *pipeline.Asset) error {
	args := d.Called(ctx, p, t)
	return args.Error(0)
}

func TestLocal_RunSingleTask(t *testing.T) {
	t.Parallel()

	p := &pipeline.Pipeline{}
	asset := &pipeline.Asset{
		Name: "task1",
		Type: "test",
	}
	instance := &scheduler.TaskInstance{
		Task: asset,
	}

	t.Run("simple instance is executed successfully", func(t *testing.T) {
		t.Parallel()

		mockOperator := new(mockOperator)
		mockOperator.On("RunTask", mock.Anything, p, asset).
			Return(nil)

		l := Sequential{
			TaskTypeMap: map[string]Operator{
				"test": mockOperator,
			},
		}

		err := l.RunSingleTask(context.Background(), p, instance)

		assert.NoError(t, err)
		mockOperator.AssertExpectations(t)
	})

	t.Run("missing instance is rejected", func(t *testing.T) {
		t.Parallel()

		mockOperator := new(mockOperator)

		l := Sequential{
			TaskTypeMap: map[string]Operator{
				"some-other-instance": mockOperator,
			},
		}

		err := l.RunSingleTask(context.Background(), p, instance)

		assert.Error(t, err)
		mockOperator.AssertExpectations(t)
	})

	t.Run("missing instance is rejected", func(t *testing.T) {
		t.Parallel()

		mockOperator := new(mockOperator)
		mockOperator.On("RunTask", mock.Anything, p, asset).
			Return(errors.New("some error occurred"))

		l := Sequential{
			TaskTypeMap: map[string]Operator{
				"test": mockOperator,
			},
		}

		err := l.RunSingleTask(context.Background(), p, instance)

		assert.Error(t, err)
		mockOperator.AssertExpectations(t)
	})
}
