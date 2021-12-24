package pipeline_test

import (
	"path/filepath"
	"testing"

	"github.com/datablast-analytics/blast-cli/pkg/pipeline"
	"github.com/stretchr/testify/require"
)

func Test_createTaskFromFile(t *testing.T) {
	type args struct {
		filePath string
	}

	absPath := func(path string) string {
		absolutePath, _ := filepath.Abs(path)
		return absolutePath
	}

	tests := []struct {
		name    string
		args    args
		want    *pipeline.Task
		wantErr bool
	}{
		{
			name: "file does not exist",
			args: args{
				filePath: "testdata/comments/some-file-that-doesnt-exist.sql",
			},
			wantErr: true,
		},
		{
			name: "existing file with no comments is skipped",
			args: args{
				filePath: "testdata/comments/nocomments.py",
			},
			wantErr: false,
		},
		{
			name: "SQL file parsed",
			args: args{
				filePath: "testdata/comments/test.sql",
			},
			want: &pipeline.Task{
				Name:        "some-sql-task",
				Description: "some description goes here",
				Type:        "bq.sql",
				ExecutableFile: pipeline.ExecutableFile{
					Name: "test.sql",
					Path: absPath("testdata/comments/test.sql"),
				},
				Parameters: map[string]string{
					"param1": "first-parameter",
					"param2": "second-parameter",
				},
				Connections: map[string]string{
					"conn1": "first-connection",
					"conn2": "second-connection",
				},
				DependsOn: []string{"task1", "task2", "task3", "task4", "task5", "task3"},
			},
		},
		{
			name: "Python file parsed",
			args: args{
				filePath: absPath("testdata/comments/test.py"), // giving an absolute path here tests the case of double-absolute paths
			},
			want: &pipeline.Task{
				Name:        "some-python-task",
				Description: "some description goes here",
				Type:        "bq.sql",
				ExecutableFile: pipeline.ExecutableFile{
					Name: "test.py",
					Path: absPath("testdata/comments/test.py"),
				},
				Parameters: map[string]string{
					"param1": "first-parameter",
					"param2": "second-parameter",
					"param3": "third-parameter",
				},
				Connections: map[string]string{
					"conn1": "first-connection",
					"conn2": "second-connection",
				},
				DependsOn: []string{"task1", "task2", "task3", "task4", "task5", "task3"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := pipeline.CreateTaskFromFileComments(tt.args.filePath)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tt.want, got)
		})
	}
}
