package pipeline_test

import (
	"path/filepath"
	"testing"

	"github.com/datablast-analytics/blast/pkg/pipeline"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
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
	expectedPipeline := &pipeline.Pipeline{
		Name:     "first-pipeline",
		LegacyID: "first-pipeline",
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
		Tasks: []*pipeline.Asset{
			{
				Name:        "hello-world",
				Description: "This is a hello world task",
				Type:        "bash",
				ExecutableFile: pipeline.ExecutableFile{
					Name:    "hello.sh",
					Path:    absPath("testdata/pipeline/first-pipeline/tasks/task1/hello.sh"),
					Content: mustRead(t, "testdata/pipeline/first-pipeline/tasks/task1/hello.sh"),
				},
				DefinitionFile: pipeline.TaskDefinitionFile{
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
				Columns:   map[string]pipeline.Column{},
			},
			{
				Name: "second-task",
				Type: "bq.transfer",
				Parameters: map[string]string{
					"transfer_config_id": "some-uuid",
					"project_id":         "a-new-project-id",
					"location":           "europe-west1",
				},
				DefinitionFile: pipeline.TaskDefinitionFile{
					Name: "task.yaml",
					Path: absPath("testdata/pipeline/first-pipeline/tasks/task2/task.yaml"),
					Type: pipeline.YamlTask,
				},
				Columns: map[string]pipeline.Column{},
			},
			{
				Name:        "some-python-task",
				Description: "some description goes here",
				Type:        "python",
				ExecutableFile: pipeline.ExecutableFile{
					Name:    "test.py",
					Path:    absPath("testdata/pipeline/first-pipeline/tasks/test.py"),
					Content: mustRead(t, "testdata/pipeline/first-pipeline/tasks/test.py"),
				},
				DefinitionFile: pipeline.TaskDefinitionFile{
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
				Columns:   map[string]pipeline.Column{},
			},
			{
				Name:        "some-sql-task",
				Description: "some description goes here",
				Type:        "bq.sql",
				ExecutableFile: pipeline.ExecutableFile{
					Name:    "test.sql",
					Path:    absPath("testdata/pipeline/first-pipeline/tasks/test.sql"),
					Content: mustRead(t, "testdata/pipeline/first-pipeline/tasks/test.sql"),
				},
				DefinitionFile: pipeline.TaskDefinitionFile{
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
				Columns:   map[string]pipeline.Column{},
			},
		},
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
			name: "should create pipeline from path",
			fields: fields{
				tasksDirectoryName: "tasks",
				commentTaskCreator: pipeline.CreateTaskFromFileComments(afero.NewOsFs()),
				yamlTaskCreator:    pipeline.CreateTaskFromYamlDefinition(afero.NewOsFs()),
			},
			args: args{
				pathToPipeline: "testdata/pipeline/first-pipeline",
			},
			want:    expectedPipeline,
			wantErr: false,
		},
		{
			name: "should create pipeline from path even if pipeline.yml is given as a path",
			fields: fields{
				tasksDirectoryName: "tasks",
				commentTaskCreator: pipeline.CreateTaskFromFileComments(afero.NewOsFs()),
				yamlTaskCreator:    pipeline.CreateTaskFromYamlDefinition(afero.NewOsFs()),
			},
			args: args{
				pathToPipeline: "testdata/pipeline/first-pipeline/pipeline.yml",
			},
			want:    expectedPipeline,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			builderConfig := pipeline.BuilderConfig{
				PipelineFileName:    "pipeline.yml",
				TasksDirectoryNames: []string{tt.fields.tasksDirectoryName},
				TasksFileSuffixes:   []string{"task.yml", "task.yaml"},
			}

			p := pipeline.NewBuilder(builderConfig, tt.fields.yamlTaskCreator, tt.fields.commentTaskCreator, afero.NewOsFs())

			got, err := p.CreatePipelineFromPath(tt.args.pathToPipeline)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			if tt.want == nil {
				return
			}

			assert.Equal(t, tt.want.Name, got.Name)
			assert.Equal(t, tt.want.LegacyID, got.LegacyID)
			assert.Equal(t, tt.want.Schedule, got.Schedule)
			assert.Equal(t, tt.want.DefinitionFile, got.DefinitionFile)
			assert.Equal(t, tt.want.DefaultConnections, got.DefaultConnections)
			assert.Equal(t, tt.want.DefaultParameters, got.DefaultParameters)
			assert.Equal(t, tt.want.Tasks, got.Tasks)
		})
	}
}

func TestTask_RelativePathToPipelineRoot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		pipeline *pipeline.Pipeline
		task     *pipeline.Asset
		want     string
	}{
		{
			name: "simple relative path returned",
			pipeline: &pipeline.Pipeline{
				DefinitionFile: pipeline.DefinitionFile{
					Path: "/users/user1/pipelines/pipeline1/pipeline.yml",
				},
			},
			task: &pipeline.Asset{
				Name: "test-task",
				DefinitionFile: pipeline.TaskDefinitionFile{
					Path: "/users/user1/pipelines/pipeline1/tasks/task-folder/task1.sql",
				},
			},
			want: "tasks/task-folder/task1.sql",
		},
		{
			name: "relative path is calculated even if the tasks are on a parent folder",
			pipeline: &pipeline.Pipeline{
				DefinitionFile: pipeline.DefinitionFile{
					Path: "/users/user1/pipelines/pipeline1/pipeline.yml",
				},
			},
			task: &pipeline.Asset{
				Name: "test-task",
				DefinitionFile: pipeline.TaskDefinitionFile{
					Path: "/users/user1/pipelines/task-folder/task1.sql",
				},
			},
			want: "../task-folder/task1.sql",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.pipeline.RelativeTaskPath(tt.task))
		})
	}
}

func TestPipeline_HasTaskType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		pipeline *pipeline.Pipeline
		taskType string
		want     bool
	}{
		{
			name: "existing task type is found",
			pipeline: &pipeline.Pipeline{
				DefinitionFile: pipeline.DefinitionFile{
					Path: "/users/user1/pipelines/pipeline1/pipeline.yml",
				},
				TasksByType: map[string][]*pipeline.Asset{
					"type1": {},
					"type2": {},
					"type3": {},
				},
			},
			taskType: "type1",
			want:     true,
		},
		{
			name: "missing task type is returned as false",
			pipeline: &pipeline.Pipeline{
				DefinitionFile: pipeline.DefinitionFile{
					Path: "/users/user1/pipelines/pipeline1/pipeline.yml",
				},
				TasksByType: map[string][]*pipeline.Asset{
					"type1": {},
					"type2": {},
					"type3": {},
				},
			},
			taskType: "some-other-type",
			want:     false,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.pipeline.HasTaskType(tt.taskType))
		})
	}
}
