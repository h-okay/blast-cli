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

func TestNotNullCheck_Check(t *testing.T) {
	t.Parallel()

	type fields struct {
		q *mockQuerierWithResult
	}

	instance := &scheduler.ColumnTestInstance{
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
	expectedQuery := &query.Query{Query: "SELECT count(*) FROM `dataset.test_asset` WHERE `test_column` IS NULL"}
	tests := []struct {
		name    string
		setup   func(f fields)
		ti      *scheduler.ColumnTestInstance
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "failed to run query",
			setup: func(f fields) {
				f.q.On("Select", mock.Anything, expectedQuery).
					Return(nil, assert.AnError).
					Once()
			},
			ti:      instance,
			wantErr: assert.Error,
		},
		{
			name: "multiple results are returned",
			setup: func(f fields) {
				f.q.On("Select", mock.Anything, expectedQuery).
					Return([][]interface{}{{1}, {2}}, nil).
					Once()
			},
			ti:      instance,
			wantErr: assert.Error,
		},
		{
			name: "null values found",
			setup: func(f fields) {
				f.q.On("Select", mock.Anything, expectedQuery).
					Return([][]interface{}{{5}}, nil).
					Once()
			},
			ti: instance,
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "column `test_column` has 5 null values")
				return true
			},
		},
		{
			name: "null values found with int64 results",
			setup: func(f fields) {
				f.q.On("Select", mock.Anything, expectedQuery).
					Return([][]interface{}{{5}}, nil).
					Once()
			},
			ti: instance,
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "column `test_column` has 5 null values")
				return true
			},
		},
		{
			name: "no null values found, test passed",
			setup: func(f fields) {
				f.q.On("Select", mock.Anything, expectedQuery).
					Return([][]interface{}{{0}}, nil).
					Once()
			},
			ti:      instance,
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			q := new(mockQuerierWithResult)
			fields := fields{
				q: q,
			}
			if tt.setup != nil {
				tt.setup(fields)
			}

			n := NotNullCheck{
				q: fields.q,
			}
			tt.wantErr(t, n.Check(context.Background(), tt.ti))
			defer q.AssertExpectations(t)
		})
	}
}
