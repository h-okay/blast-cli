package bigquery

import (
	"context"
	"testing"

	"github.com/datablast-analytics/blast/pkg/pipeline"
	"github.com/datablast-analytics/blast/pkg/query"
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

type mockMaterializer struct {
	mock.Mock
}

func (m *mockMaterializer) Render(t *pipeline.Asset, query string) (string, error) {
	res := m.Called(t, query)
	return res.Get(0).(string), res.Error(1)
}

func TestBasicOperator_RunTask(t *testing.T) {
	t.Parallel()

	type args struct {
		t *pipeline.Asset
	}

	tests := []struct {
		name              string
		setupQueries      func(m *mockQuerier)
		setupExtractor    func(m *mockExtractor)
		setupMaterializer func(m *mockMaterializer)
		args              args
		wantErr           bool
	}{
		{
			name: "failed to extract queries",
			setupExtractor: func(m *mockExtractor) {
				m.On("ExtractQueriesFromFile", "test-file.sql").
					Return([]*query.Query{}, errors.New("failed to extract queries"))
			},
			args: args{
				t: &pipeline.Asset{
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
				t: &pipeline.Asset{
					ExecutableFile: pipeline.ExecutableFile{
						Path: "test-file.sql",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "multiple queries found but materialization is enabled, should fail",
			setupExtractor: func(m *mockExtractor) {
				m.On("ExtractQueriesFromFile", "test-file.sql").
					Return([]*query.Query{
						{Query: "query 1"},
						{Query: "query 2"},
					}, nil)
			},
			args: args{
				t: &pipeline.Asset{
					ExecutableFile: pipeline.ExecutableFile{
						Path: "test-file.sql",
					},
					Materialization: pipeline.Materialization{
						Type: pipeline.MaterializationTypeTable,
					},
				},
			},
			wantErr: true,
		},
		{
			name: "query returned an error",
			setupExtractor: func(m *mockExtractor) {
				m.On("ExtractQueriesFromFile", "test-file.sql").
					Return([]*query.Query{
						{Query: "select * from users"},
					}, nil)
			},
			setupMaterializer: func(m *mockMaterializer) {
				m.On("Render", mock.Anything, "select * from users").
					Return("select * from users", nil)
			},
			setupQueries: func(m *mockQuerier) {
				m.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "select * from users"}).
					Return(errors.New("failed to run query"))
			},
			args: args{
				t: &pipeline.Asset{
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
			setupMaterializer: func(m *mockMaterializer) {
				m.On("Render", mock.Anything, "select * from users").
					Return("select * from users", nil)
			},
			setupQueries: func(m *mockQuerier) {
				m.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "select * from users"}).
					Return(nil)
			},
			args: args{
				t: &pipeline.Asset{
					ExecutableFile: pipeline.ExecutableFile{
						Path: "test-file.sql",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "query successfully executed with materialization",
			setupExtractor: func(m *mockExtractor) {
				m.On("ExtractQueriesFromFile", "test-file.sql").
					Return([]*query.Query{
						{Query: "select * from users"},
					}, nil)
			},
			setupMaterializer: func(m *mockMaterializer) {
				m.On("Render", mock.Anything, "select * from users").
					Return("CREATE TABLE x AS select * from users", nil)
			},
			setupQueries: func(m *mockQuerier) {
				m.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "CREATE TABLE x AS select * from users"}).
					Return(nil)
			},
			args: args{
				t: &pipeline.Asset{
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

			mat := new(mockMaterializer)
			if tt.setupMaterializer != nil {
				tt.setupMaterializer(mat)
			}

			o := BasicOperator{
				client:       client,
				extractor:    extractor,
				materializer: mat,
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
