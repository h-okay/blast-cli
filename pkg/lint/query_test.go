package lint

import (
	"context"
	"errors"
	"testing"

	"github.com/datablast-analytics/blast-cli/pkg/pipeline"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockValidator struct {
	mock.Mock
}

func (m *mockValidator) IsValid(ctx context.Context, query string) (bool, error) {
	res := m.Called(ctx, query)
	return res.Bool(0), res.Error(1)
}

type mockExtractor struct {
	mock.Mock
}

func (m *mockExtractor) ExtractQueriesFromFile(filepath string) ([]string, error) {
	res := m.Called(filepath)
	return res.Get(0).([]string), res.Error(1)
}

func TestQueryValidatorRule_Validate(t *testing.T) {
	t.Parallel()

	noIssues := make([]*Issue, 0)
	taskType := "someTaskType"

	tests := []struct {
		name           string
		p              *pipeline.Pipeline
		setupValidator func(m *mockValidator)
		setupExtractor func(m *mockExtractor)
		want           []*Issue
		wantErr        bool
	}{
		{
			name: "no tasks to execute",
			p: &pipeline.Pipeline{
				Tasks: []*pipeline.Task{},
			},
			want:    noIssues,
			wantErr: false,
		},
		{
			name: "no tasks from task type to execute",
			p: &pipeline.Pipeline{
				Tasks: []*pipeline.Task{
					{
						Type: "someOtherTaskType",
					},
					{
						Type: "yet another task type",
					},
				},
			},
			want:    noIssues,
			wantErr: false,
		},
		{
			name: "a task to extract, but query extractor fails",
			p: &pipeline.Pipeline{
				Tasks: []*pipeline.Task{
					{
						Type: "someOtherTaskType",
					},
					{
						Type: taskType,
						ExecutableFile: pipeline.ExecutableFile{
							Path: "path/to/file-with-no-queries.sql",
						},
					},
				},
			},
			setupExtractor: func(m *mockExtractor) {
				m.On("ExtractQueriesFromFile", "path/to/file-with-no-queries.sql").
					Return([]string{}, errors.New("something failed"))
			},
			want: []*Issue{
				{
					Task: &pipeline.Task{
						Type: taskType,
						ExecutableFile: pipeline.ExecutableFile{
							Path: "path/to/file-with-no-queries.sql",
						},
					},
					Description: "Cannot read executable file 'path/to/file-with-no-queries.sql': something failed",
				},
			},
		},
		{
			name: "a task to extract, but no queries in it",
			p: &pipeline.Pipeline{
				Tasks: []*pipeline.Task{
					{
						Type: "someOtherTaskType",
					},
					{
						Type: taskType,
						ExecutableFile: pipeline.ExecutableFile{
							Path: "path/to/file-with-no-queries.sql",
						},
					},
				},
			},
			setupExtractor: func(m *mockExtractor) {
				m.On("ExtractQueriesFromFile", "path/to/file-with-no-queries.sql").Return([]string{}, nil)
			},
			want: []*Issue{
				{
					Task: &pipeline.Task{
						Type: taskType,
						ExecutableFile: pipeline.ExecutableFile{
							Path: "path/to/file-with-no-queries.sql",
						},
					},
					Description: "No queries found in executable file 'path/to/file-with-no-queries.sql'",
				},
			},
		},
		{
			name: "two tasks to extract, 3 queries in each, one invalid",
			p: &pipeline.Pipeline{
				Tasks: []*pipeline.Task{
					{
						Type: "someOtherTaskType",
					},
					{
						Type: taskType,
						ExecutableFile: pipeline.ExecutableFile{
							Path: "path/to/file1.sql",
						},
					},
					{
						Type: taskType,
						ExecutableFile: pipeline.ExecutableFile{
							Path: "path/to/file2.sql",
						},
					},
				},
			},
			setupExtractor: func(m *mockExtractor) {
				m.On("ExtractQueriesFromFile", "path/to/file1.sql").
					Return([]string{"query11", "query12", "query13"}, nil)
				m.On("ExtractQueriesFromFile", "path/to/file2.sql").
					Return([]string{"query21", "query22", "query23"}, nil)
			},
			setupValidator: func(m *mockValidator) {
				m.On("IsValid", mock.Anything, "query11").Return(true, nil)
				m.On("IsValid", mock.Anything, "query12").Return(false, errors.New("invalid query query12"))
				m.On("IsValid", mock.Anything, "query13").Return(true, nil)

				m.On("IsValid", mock.Anything, "query21").Return(true, nil)
				m.On("IsValid", mock.Anything, "query22").Return(true, nil)
				m.On("IsValid", mock.Anything, "query23").Return(false, nil)
			},
			want: []*Issue{
				{
					Task: &pipeline.Task{
						Type: taskType,
						ExecutableFile: pipeline.ExecutableFile{
							Path: "path/to/file1.sql",
						},
					},
					Description: "Invalid query found at 'query12': invalid query query12",
				},
				{
					Task: &pipeline.Task{
						Type: taskType,
						ExecutableFile: pipeline.ExecutableFile{
							Path: "path/to/file2.sql",
						},
					},
					Description: "Query 'query23' is invalid",
				},
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			validator := new(mockValidator)
			extractor := new(mockExtractor)

			if tt.setupValidator != nil {
				tt.setupValidator(validator)
			}

			if tt.setupExtractor != nil {
				tt.setupExtractor(extractor)
			}

			q := &QueryValidatorRule{
				TaskType:    taskType,
				Validator:   validator,
				Extractor:   extractor,
				WorkerCount: 8,
			}
			got, err := q.Validate(tt.p)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			require.ElementsMatchf(t, tt.want, got, "the wanted and got values dont match for issues")
			validator.AssertExpectations(t)
			extractor.AssertExpectations(t)
		})
	}
}
