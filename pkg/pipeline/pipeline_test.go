package pipeline_test

import (
	"path/filepath"
	"testing"

	"github.com/datablast-analytics/blast-cli/pkg/pipeline"
	"github.com/stretchr/testify/require"
)

func Test_pipelineBuilder_CreatePipelineFromPath(t *testing.T) {
	t.Parallel()

	absPath := func(path string) string {
		absolutePath, _ := filepath.Abs(path)
		return absolutePath
	}

	type fields struct {
		tasksDirectoryName string
		yamlTaskCreator    pipeline.TaskCreator
		commentTaskCreator pipeline.TaskCreator
	}
	type args struct {
		pathToPipeline string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *pipeline.Pipeline
		wantErr bool
	}{
		{
			name: "missing path should error",
			fields: fields{
				tasksDirectoryName: "tasks",
			},
			args: args{
				pathToPipeline: "testdata/some/path/that/doesnt/exist",
			},
			wantErr: true,
		},
		{
			name: "missing path for the tasks should error",
			fields: fields{
				tasksDirectoryName: "some-missing-directory-name",
			},
			args: args{
				pathToPipeline: "testdata/pipeline/first-pipeline",
			},
			wantErr: true,
		},
		{
			name: "should create pipeline from path",
			fields: fields{
				tasksDirectoryName: "tasks",
				commentTaskCreator: pipeline.CreateTaskFromFileComments,
				yamlTaskCreator:    pipeline.CreateTaskFromYamlDefinition,
			},
			args: args{
				pathToPipeline: "testdata/pipeline/first-pipeline",
			},
			want: &pipeline.Pipeline{
				Name:     "first-pipeline",
				Schedule: "",
				DefinitionFile: pipeline.DefinitionFile{
					Name: "pipeline.yml",
					Path: absPath("testdata/pipeline/first-pipeline/pipeline.yml"),
				},
				DefaultParameters: map[string]string{
					"param1": "value1",
					"param2": "value2",
				},
				DefaultConnections: map[string]string{
					"slack":           "slack-connection",
					"gcpConnectionId": "gcp-connection-id-here",
				},
				Tasks: []*pipeline.Task{
					{
						Name:        "hello-world",
						Description: "This is a hello world task",
						Type:        "bash",
						ExecutableFile: pipeline.ExecutableFile{
							Name: "hello.sh",
							Path: absPath("testdata/pipeline/first-pipeline/tasks/task1/hello.sh"),
						},
						DefinitionFile: pipeline.DefinitionFile{
							Name: "task.yml",
							Path: absPath("testdata/pipeline/first-pipeline/tasks/task1/task.yml"),
							Type: pipeline.YamlTask,
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
					{
						Name: "second-task",
						Type: "bq.transfer",
						Parameters: map[string]string{
							"transfer_config_id": "some-uuid",
							"project_id":         "a-new-project-id",
							"location":           "europe-west1",
						},
						DefinitionFile: pipeline.DefinitionFile{
							Name: "task.yml",
							Path: absPath("testdata/pipeline/first-pipeline/tasks/task2/task.yml"),
							Type: pipeline.YamlTask,
						},
					},
					{
						Name:        "some-python-task",
						Description: "some description goes here",
						Type:        "python",
						ExecutableFile: pipeline.ExecutableFile{
							Name: "test.py",
							Path: absPath("testdata/pipeline/first-pipeline/tasks/test.py"),
						},
						DefinitionFile: pipeline.DefinitionFile{
							Name: "test.py",
							Path: absPath("testdata/pipeline/first-pipeline/tasks/test.py"),
							Type: pipeline.CommentTask,
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
					{
						Name:        "some-sql-task",
						Description: "some description goes here",
						Type:        "bq.sql",
						ExecutableFile: pipeline.ExecutableFile{
							Name: "test.sql",
							Path: absPath("testdata/pipeline/first-pipeline/tasks/test.sql"),
						},
						DefinitionFile: pipeline.DefinitionFile{
							Name: "test.sql",
							Path: absPath("testdata/pipeline/first-pipeline/tasks/test.sql"),
							Type: pipeline.CommentTask,
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
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			builderConfig := pipeline.BuilderConfig{
				PipelineFileName:   "pipeline.yml",
				TasksDirectoryName: tt.fields.tasksDirectoryName,
				TasksFileName:      "task.yml",
			}

			p := pipeline.NewBuilder(builderConfig, tt.fields.yamlTaskCreator, tt.fields.commentTaskCreator)

			got, err := p.CreatePipelineFromPath(tt.args.pathToPipeline)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tt.want, got)
		})
	}
}
