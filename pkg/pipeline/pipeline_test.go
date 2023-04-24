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

	asset1 := &pipeline.Asset{
		Name:        "task1",
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
		Connection: "conn1",
		DependsOn:  []string{"gcs-to-bq"},
		Columns:    map[string]pipeline.Column{},
	}

	asset2 := &pipeline.Asset{
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
	}

	asset3 := &pipeline.Asset{
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
		Connection: "first-connection",
		DependsOn:  []string{"task1", "task2", "task3", "task4", "task5", "task3"},
		Columns:    map[string]pipeline.Column{},
	}
	asset3.AddUpstream(asset1)
	asset1.AddDownstream(asset3)

	asset4 := &pipeline.Asset{
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
		Connection: "conn2",
		DependsOn:  []string{"task1", "task2", "task3", "task4", "task5", "task3"},
		Columns:    map[string]pipeline.Column{},
	}
	asset4.AddUpstream(asset1)
	asset1.AddDownstream(asset4)

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
		Tasks: []*pipeline.Asset{asset1, asset2, asset3, asset4},
	}
	fs := afero.NewOsFs()
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
				commentTaskCreator: pipeline.CreateTaskFromFileComments(fs),
				yamlTaskCreator:    pipeline.CreateTaskFromYamlDefinition(fs),
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
				commentTaskCreator: pipeline.CreateTaskFromFileComments(fs),
				yamlTaskCreator:    pipeline.CreateTaskFromYamlDefinition(fs),
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

			p := pipeline.NewBuilder(builderConfig, tt.fields.yamlTaskCreator, tt.fields.commentTaskCreator, fs)

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

			for i, asset := range tt.want.Tasks {
				gotAsset := got.Tasks[i]
				assert.EqualExportedValues(t, *asset, *gotAsset)

				gotAssetUpstreams := gotAsset.GetUpstream()
				assert.Equal(t, len(asset.GetUpstream()), len(gotAssetUpstreams))
				for upstreamIdx, u := range asset.GetUpstream() {
					assert.EqualExportedValues(t, *u, *gotAssetUpstreams[upstreamIdx])
				}

				gotAssetDownstreams := gotAsset.GetDownstream()
				assert.Equal(t, len(asset.GetDownstream()), len(gotAssetDownstreams))
				for idx, d := range asset.GetDownstream() {
					assert.EqualExportedValues(t, *d, *gotAssetDownstreams[idx])
				}
			}
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
			assert.Equal(t, tt.want, tt.pipeline.RelativeAssetPath(tt.task))
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
				TasksByType: map[pipeline.AssetType][]*pipeline.Asset{
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
				TasksByType: map[pipeline.AssetType][]*pipeline.Asset{
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
			assert.Equal(t, tt.want, tt.pipeline.HasAssetType(pipeline.AssetType(tt.taskType)))
		})
	}
}

func TestAsset_AddUpstream(t *testing.T) {
	t.Parallel()

	asset1 := &pipeline.Asset{Name: "asset1"}
	asset2 := &pipeline.Asset{Name: "asset2"}
	asset3 := &pipeline.Asset{Name: "asset3"}
	asset4 := &pipeline.Asset{Name: "asset4"}

	connect := func(upstream *pipeline.Asset, downstream *pipeline.Asset) {
		t.Helper()
		upstream.AddDownstream(downstream)
		downstream.AddUpstream(upstream)
	}

	connect(asset1, asset2)
	connect(asset2, asset3)
	connect(asset1, asset3)
	connect(asset3, asset4)

	assert.ElementsMatch(t, []*pipeline.Asset{asset2, asset1}, asset3.GetUpstream())
	assert.ElementsMatch(t, []*pipeline.Asset{asset4}, asset3.GetDownstream())

	assert.ElementsMatch(t, []*pipeline.Asset{asset1}, asset2.GetUpstream())
	assert.ElementsMatch(t, []*pipeline.Asset{asset3}, asset2.GetDownstream())

	assert.ElementsMatch(t, []*pipeline.Asset{}, asset1.GetUpstream())
	assert.ElementsMatch(t, []*pipeline.Asset{asset2, asset3}, asset1.GetDownstream())
	assert.ElementsMatch(t, []*pipeline.Asset{asset2, asset3, asset4}, asset1.GetFullDownstream())

	assert.ElementsMatch(t, []*pipeline.Asset{asset3}, asset4.GetUpstream())
	assert.ElementsMatch(t, []*pipeline.Asset{asset1, asset2, asset3}, asset4.GetFullUpstream())
	assert.ElementsMatch(t, []*pipeline.Asset{}, asset4.GetDownstream())
	assert.ElementsMatch(t, []*pipeline.Asset{}, asset4.GetFullDownstream())
}

func TestPipeline_GetAssetByPath(t *testing.T) {
	t.Parallel()

	fs := afero.NewCacheOnReadFs(afero.NewOsFs(), afero.NewMemMapFs(), 0)
	config := pipeline.BuilderConfig{
		PipelineFileName:    "pipeline.yml",
		TasksDirectoryNames: []string{"tasks", "assets"},
		TasksFileSuffixes:   []string{"task.yml", "task.yaml"},
	}
	builder := pipeline.NewBuilder(config, pipeline.CreateTaskFromYamlDefinition(fs), pipeline.CreateTaskFromFileComments(fs), fs)
	p, err := builder.CreatePipelineFromPath("./testdata/pipeline/first-pipeline")
	assert.NoError(t, err)

	absPath := func(path string) string {
		absolutePath, err := filepath.Abs(path)
		assert.NoError(t, err)
		return absolutePath
	}

	asset := p.GetAssetByPath("testdata/pipeline/first-pipeline/tasks/task1/task.yml")
	assert.NotNil(t, asset)
	assert.Equal(t, "task1", asset.Name)

	asset = p.GetAssetByPath("./testdata/pipeline/first-pipeline/tasks/task1/task.yml")
	assert.NotNil(t, asset)
	assert.Equal(t, "task1", asset.Name)

	asset = p.GetAssetByPath(absPath("./testdata/pipeline/first-pipeline/tasks/task1/task.yml"))
	assert.NotNil(t, asset)
	assert.Equal(t, "task1", asset.Name)

	asset = p.GetAssetByPath(absPath("../pipeline/testdata/../testdata/pipeline/first-pipeline/tasks/task1/task.yml"))
	assert.NotNil(t, asset)
	assert.Equal(t, "task1", asset.Name)
}
