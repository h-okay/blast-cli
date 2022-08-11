package executor

import (
	"testing"

	"github.com/datablast-analytics/blast-cli/pkg/pipeline"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockOperator struct {
	mock.Mock
}

func (d *mockOperator) RunTask(p pipeline.Pipeline, t pipeline.Task) error {
	args := d.Called(p, t)
	return args.Error(0)
}

func TestLocal_RunSingleTask(t *testing.T) {
	t.Parallel()

	p := pipeline.Pipeline{}
	task := pipeline.Task{
		Name: "task1",
		Type: "test",
	}

	t.Run("simple task is executed successfully", func(t *testing.T) {
		t.Parallel()

		mockOperator := new(mockOperator)
		mockOperator.On("RunTask", p, task).
			Return(nil)

		l := Sequential{
			TaskTypeMap: map[string]operator{
				"test": mockOperator,
			},
		}

		err := l.RunSingleTask(p, task)

		assert.NoError(t, err)
		mockOperator.AssertExpectations(t)
	})

	t.Run("missing task is rejected", func(t *testing.T) {
		t.Parallel()

		mockOperator := new(mockOperator)

		l := Sequential{
			TaskTypeMap: map[string]operator{
				"some-other-task": mockOperator,
			},
		}

		err := l.RunSingleTask(p, task)

		assert.Error(t, err)
		mockOperator.AssertExpectations(t)
	})

	t.Run("missing task is rejected", func(t *testing.T) {
		t.Parallel()

		mockOperator := new(mockOperator)
		mockOperator.On("RunTask", p, task).
			Return(errors.New("some error occurred"))

		l := Sequential{
			TaskTypeMap: map[string]operator{
				"test": mockOperator,
			},
		}

		err := l.RunSingleTask(p, task)

		assert.Error(t, err)
		mockOperator.AssertExpectations(t)
	})
}
