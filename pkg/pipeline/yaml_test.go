package pipeline_test

import (
	"path/filepath"
	"testing"

	"github.com/datablast-analytics/blast-cli/pkg/pipeline"
	"github.com/stretchr/testify/require"
)

func TestCreateTaskFromYamlDefinition(t *testing.T) {
	t.Parallel()

	absPath := func(path string) string {
		absolutePath, _ := filepath.Abs(path)
		return absolutePath
	}

	type args struct {
		filePath string
	}

	tests := []struct {
		name    string
		args    args
		want    *pipeline.Task
		wantErr bool
	}{
		{
			name: "fails for paths that do not exist",
			args: args{
				filePath: "testdata/some/path/that/doesnt/exist",
			},
			wantErr: true,
		},
		{
			name: "fails for non-yaml files",
			args: args{
				filePath: "testdata/yaml/task1/hello.sh",
			},
			wantErr: true,
		},
		{
			name: "reads a valid simple file",
			args: args{
				filePath: "testdata/yaml/task1/task.yml",
			},
			want: &pipeline.Task{
				Name:        "hello-world",
				Description: "This is a hello world task",
				Type:        "bash",
				ExecutableFile: pipeline.ExecutableFile{
					Name: "hello.sh",
					Path: absPath("testdata/yaml/task1/hello.sh"),
				},
				Parameters: map[string]string{
					"param1": "value1",
					"param2": "value2",
				},
				Connections: map[string]string{
					"conn1": "first connection",
					"conn2": "second connection",
				},
				DependsOn: []string{"gcs-to-bq"},
			},
		},
		{
			name: "nested runfile paths work correctly",
			args: args{
				filePath: "testdata/yaml/task-with-nested/task.yml",
			},
			want: &pipeline.Task{
				Name:        "hello-world",
				Description: "This is a hello world task",
				Type:        "bash",
				ExecutableFile: pipeline.ExecutableFile{
					Name: "hello.sh",
					Path: absPath("testdata/yaml/task-with-nested/some/dir/hello.sh"),
				},
				Parameters: map[string]string{
					"param1": "value1",
					"param2": "value2",
				},
				Connections: map[string]string{
					"conn1": "first connection",
					"conn2": "second connection",
				},
				DependsOn: []string{"gcs-to-bq"},
			},
		},
		{
			name: "top-level runfile paths are still joined correctly",
			args: args{
				filePath: "testdata/yaml/task-with-toplevel-runfile/task.yml",
			},
			want: &pipeline.Task{
				Name:        "hello-world",
				Description: "This is a hello world task",
				Type:        "bash",
				ExecutableFile: pipeline.ExecutableFile{
					Name: "hello.sh",
					Path: absPath("testdata/yaml/task-with-toplevel-runfile/hello.sh"),
				},
				Parameters: map[string]string{
					"param1": "value1",
					"param2": "value2",
				},
				Connections: map[string]string{
					"conn1": "first connection",
					"conn2": "second connection",
				},
				DependsOn: []string{"gcs-to-bq"},
				Schedule:  pipeline.TaskSchedule{Days: []string{"sunday", "monday", "tuesday"}},
			},
		},
		{
			name: "the ones with missing runfile are ignored",
			args: args{
				filePath: "testdata/yaml/task-with-no-runfile/task.yml",
			},
			want: &pipeline.Task{
				Name:        "hello-world",
				Description: "This is a hello world task",
				Type:        "bash",
				Parameters: map[string]string{
					"param1": "value1",
					"param2": "value2",
				},
				Connections: map[string]string{
					"conn1": "first connection",
					"conn2": "second connection",
				},
				DependsOn: []string{"gcs-to-bq"},
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := pipeline.CreateTaskFromYamlDefinition(tt.args.filePath)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tt.want, got)
		})
	}
}
