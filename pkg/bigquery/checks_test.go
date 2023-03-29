package bigquery

import (
	"context"
	"testing"

	"github.com/datablast-analytics/blast/pkg/pipeline"
	"github.com/datablast-analytics/blast/pkg/query"
	"github.com/datablast-analytics/blast/pkg/scheduler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockQuerierWithResult struct {
	mock.Mock
}

func (m *mockQuerierWithResult) Select(ctx context.Context, q *query.Query) ([][]interface{}, error) {
	args := m.Called(ctx, q)
	get := args.Get(0)
	if get == nil {
		return nil, args.Error(1)
	}

	return get.([][]interface{}), args.Error(1)
}

var (
	checkError = func(message string) assert.ErrorAssertionFunc {
		return func(t assert.TestingT, err error, i ...interface{}) bool {
			return assert.EqualError(t, err, message)
		}
	}

	testInstance = &scheduler.ColumnTestInstance{
		AssetInstance: &scheduler.AssetInstance{
			Asset: &pipeline.Asset{
				Name: "dataset.test_asset",
			},
		},
		Column: &pipeline.Column{
			Name: "test_column",
			Tests: []pipeline.ColumnTest{
				{
					Name: "not_null",
				},
			},
		},
		Test: &pipeline.ColumnTest{
			Name: "not_null",
		},
	}
)

func TestNotNullCheck_Check(t *testing.T) {
	t.Parallel()

	expectedQuery := &query.Query{Query: "SELECT count(*) FROM `dataset.test_asset` WHERE `test_column` IS NULL"}
	setupFunc := func(val [][]interface{}, err error) func(f NotNullCheck) {
		return func(n NotNullCheck) {
			n.q.(*mockQuerierWithResult).On("Select", mock.Anything, expectedQuery).
				Return(val, err).
				Once()
		}
	}

	tests := []struct {
		name    string
		setup   func(f NotNullCheck)
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name:    "failed to run query",
			setup:   setupFunc(nil, assert.AnError),
			wantErr: assert.Error,
		},
		{
			name:    "multiple results are returned",
			setup:   setupFunc([][]interface{}{{1}, {2}}, nil),
			wantErr: assert.Error,
		},
		{
			name:    "null values found",
			setup:   setupFunc([][]interface{}{{5}}, nil),
			wantErr: checkError("column `test_column` has 5 null values"),
		},
		{
			name:    "null values found with int64 results",
			setup:   setupFunc([][]interface{}{{int64(5)}}, nil),
			wantErr: checkError("column `test_column` has 5 null values"),
		},
		{
			name:    "no null values found, test passed",
			setup:   setupFunc([][]interface{}{{0}}, nil),
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			n := NotNullCheck{q: new(mockQuerierWithResult)}
			tt.setup(n)

			tt.wantErr(t, n.Check(context.Background(), testInstance))
			defer new(mockQuerierWithResult).AssertExpectations(t)
		})
	}
}

func TestPositiveCheck_Check(t *testing.T) {
	t.Parallel()

	expectedQuery := &query.Query{Query: "SELECT count(*) FROM `dataset.test_asset` WHERE `test_column` <= 0"}
	setupFunc := func(val [][]interface{}, err error) func(n PositiveCheck) {
		return func(n PositiveCheck) {
			n.q.(*mockQuerierWithResult).On("Select", mock.Anything, expectedQuery).
				Return(val, err).
				Once()
		}
	}

	tests := []struct {
		name    string
		setup   func(n PositiveCheck)
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name:    "failed to run query",
			setup:   setupFunc(nil, assert.AnError),
			wantErr: assert.Error,
		},
		{
			name:    "multiple results are returned",
			setup:   setupFunc([][]interface{}{{1}, {2}}, nil),
			wantErr: assert.Error,
		},
		{
			name:    "null values found",
			setup:   setupFunc([][]interface{}{{5}}, nil),
			wantErr: checkError("column `test_column` has 5 positive values"),
		},
		{
			name:    "null values found with int64 results",
			setup:   setupFunc([][]interface{}{{int64(5)}}, nil),
			wantErr: checkError("column `test_column` has 5 positive values"),
		},
		{
			name:    "no null values found, test passed",
			setup:   setupFunc([][]interface{}{{0}}, nil),
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			n := PositiveCheck{
				q: new(mockQuerierWithResult),
			}

			tt.setup(n)

			tt.wantErr(t, n.Check(context.Background(), testInstance))
			defer n.q.(*mockQuerierWithResult).AssertExpectations(t)
		})
	}
}
