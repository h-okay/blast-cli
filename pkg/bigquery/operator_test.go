package bigquery

import (
	"context"
	"testing"

	"github.com/datablast-analytics/blast-cli/pkg/pipeline"
	"github.com/datablast-analytics/blast-cli/pkg/query"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockQuerier struct {
	mock.Mock
}

func (m *mockQuerier) RunQueryWithoutResult(ctx context.Context, q *query.Query) error {
	args := m.Called(ctx, q)
	return args.Error(0)
}

type mockExtractor struct {
	mock.Mock
}

func (m *mockExtractor) ExtractQueriesFromFile(filepath string) ([]*query.Query, error) {
	res := m.Called(filepath)
	return res.Get(0).([]*query.Query), res.Error(1)
}

func TestBasicOperator_RunTask(t *testing.T) {
	t.Parallel()

	type args struct {
		t *pipeline.Task
	}

	tests := []struct {
		name           string
		setupQueries   func(m *mockQuerier)
		setupExtractor func(m *mockExtractor)
		args           args
		wantErr        bool
	}{
		{
			name: "failed to extract queries",
			setupExtractor: func(m *mockExtractor) {
				m.On("ExtractQueriesFromFile", "test-file.sql").
					Return([]*query.Query{}, errors.New("failed to extract queries"))
			},
			args: args{
				t: &pipeline.Task{
					ExecutableFile: pipeline.ExecutableFile{
						Path: "test-file.sql",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "no queries found in file",
			setupExtractor: func(m *mockExtractor) {
				m.On("ExtractQueriesFromFile", "test-file.sql").
					Return([]*query.Query{}, nil)
			},
			args: args{
				t: &pipeline.Task{
					ExecutableFile: pipeline.ExecutableFile{
						Path: "test-file.sql",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "query returned an error",
			setupExtractor: func(m *mockExtractor) {
				m.On("ExtractQueriesFromFile", "test-file.sql").
					Return([]*query.Query{
						{Query: "select * from users"},
					}, nil)
			},
			setupQueries: func(m *mockQuerier) {
				m.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "select * from users"}).
					Return(errors.New("failed to run query"))
			},
			args: args{
				t: &pipeline.Task{
					ExecutableFile: pipeline.ExecutableFile{
						Path: "test-file.sql",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "query successfully executed",
			setupExtractor: func(m *mockExtractor) {
				m.On("ExtractQueriesFromFile", "test-file.sql").
					Return([]*query.Query{
						{Query: "select * from users"},
					}, nil)
			},
			setupQueries: func(m *mockQuerier) {
				m.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "select * from users"}).
					Return(nil)
			},
			args: args{
				t: &pipeline.Task{
					ExecutableFile: pipeline.ExecutableFile{
						Path: "test-file.sql",
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := new(mockQuerier)
			if tt.setupQueries != nil {
				tt.setupQueries(client)
			}

			extractor := new(mockExtractor)
			if tt.setupExtractor != nil {
				tt.setupExtractor(extractor)
			}

			o := BasicOperator{
				client:    client,
				extractor: extractor,
			}

			err := o.RunTask(context.Background(), &pipeline.Pipeline{}, tt.args.t)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
