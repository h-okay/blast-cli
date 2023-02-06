package pipeline

import (
	"path/filepath"
	"strings"

	"github.com/datablast-analytics/blast-cli/pkg/path"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

const (
	CommentTask TaskDefinitionType = "comment"
	YamlTask    TaskDefinitionType = "yaml"
)

var supportedFileSuffixes = []string{".yml", ".yaml", ".sql", ".py"}

type (
	schedule           string
	TaskDefinitionType string
)

type ExecutableFile struct {
	Name    string
	Path    string
	Content string
}

type TaskDefinitionFile struct {
	Name string
	Path string
	Type TaskDefinitionType
}

type DefinitionFile struct {
	Name string
	Path string
}

type TaskSchedule struct {
	Days []string
}

type Notifications struct {
	Slack []SlackNotification
}

type SlackNotification struct {
	Name       string
	Connection string
	Success    string
	Failure    string
}

type MaterializationType string

const (
	MaterializationTypeNone  MaterializationType = ""
	MaterializationTypeView  MaterializationType = "view"
	MaterializationTypeTable MaterializationType = "table"
)

type MaterializationStrategy string

const (
	MaterializationStrategyNone          MaterializationStrategy = ""
	MaterializationStrategyCreateReplace MaterializationStrategy = "create+replace"
	MaterializationStrategyDeleteInsert  MaterializationStrategy = "delete+insert"
	MaterializationStrategyAppend        MaterializationStrategy = "append"
)

type Materialization struct {
	Type           MaterializationType
	Strategy       MaterializationStrategy
	PartitionBy    string
	ClusterBy      []string
	IncrementalKey string
}

type Task struct {
	Name            string
	Description     string
	Type            string
	ExecutableFile  ExecutableFile
	DefinitionFile  TaskDefinitionFile
	Parameters      map[string]string
	Connections     map[string]string
	DependsOn       []string
	Pipeline        *Pipeline
	Schedule        TaskSchedule
	Materialization Materialization
}

type Pipeline struct {
	LegacyID           string   `yaml:"id"`
	Name               string   `yaml:"name"`
	Schedule           schedule `yaml:"schedule"`
	StartDate          string   `yaml:"start_date"`
	DefinitionFile     DefinitionFile
	DefaultParameters  map[string]string `yaml:"default_parameters"`
	DefaultConnections map[string]string `yaml:"default_connections"`
	Tasks              []*Task
	Notifications      Notifications `yaml:"notifications"`

	TasksByType map[string][]*Task
	tasksByName map[string]*Task
}

func (p *Pipeline) RelativeTaskPath(t *Task) string {
	absolutePipelineRoot := filepath.Dir(p.DefinitionFile.Path)

	pipelineDirectory, err := filepath.Rel(absolutePipelineRoot, t.DefinitionFile.Path)
	if err != nil {
		return absolutePipelineRoot
	}

	return pipelineDirectory
}

func (p Pipeline) HasTaskType(taskType string) bool {
	_, ok := p.TasksByType[taskType]
	return ok
}

type TaskCreator func(path string) (*Task, error)

type BuilderConfig struct {
	PipelineFileName   string
	TasksDirectoryName string
	TasksFileSuffixes  []string
}

type builder struct {
	config             BuilderConfig
	yamlTaskCreator    TaskCreator
	commentTaskCreator TaskCreator
	fs                 afero.Fs
}

func NewBuilder(config BuilderConfig, yamlTaskCreator TaskCreator, commentTaskCreator TaskCreator, fs afero.Fs) *builder {
	return &builder{
		config:             config,
		yamlTaskCreator:    yamlTaskCreator,
		commentTaskCreator: commentTaskCreator,
		fs:                 fs,
	}
}

func (b *builder) CreatePipelineFromPath(pathToPipeline string) (*Pipeline, error) {
	pipelineFilePath := filepath.Join(pathToPipeline, b.config.PipelineFileName)
	tasksPath := filepath.Join(pathToPipeline, b.config.TasksDirectoryName)

	var pipeline Pipeline
	err := path.ReadYaml(b.fs, pipelineFilePath, &pipeline)
	if err != nil {
		return nil, errors.Wrapf(err, "error reading pipeline file at '%s'", pipelineFilePath)
	}

	// this is needed until we migrate all the pipelines to use the new naming convention
	if pipeline.Name == "" {
		pipeline.Name = pipeline.LegacyID
	}
	pipeline.TasksByType = make(map[string][]*Task)
	pipeline.tasksByName = make(map[string]*Task)

	absPipelineFilePath, err := filepath.Abs(pipelineFilePath)
	if err != nil {
		return nil, errors.Wrapf(err, "error getting absolute path for pipeline file at '%s'", pipelineFilePath)
	}

	pipeline.DefinitionFile = DefinitionFile{
		Name: filepath.Base(pipelineFilePath),
		Path: absPipelineFilePath,
	}

	taskFiles, err := path.GetAllFilesRecursive(tasksPath, supportedFileSuffixes)
	if err != nil {
		return nil, errors.Wrapf(err, "error listing Task files at '%s'", tasksPath)
	}

	for _, file := range taskFiles {
		task, err := b.CreateTaskFromFile(file)
		if err != nil {
			return nil, err
		}

		if task == nil {
			continue
		}

		pipeline.Tasks = append(pipeline.Tasks, task)

		if _, ok := pipeline.TasksByType[task.Type]; !ok {
			pipeline.TasksByType[task.Type] = make([]*Task, 0)
		}

		pipeline.TasksByType[task.Type] = append(pipeline.TasksByType[task.Type], task)
		pipeline.tasksByName[task.Name] = task
	}

	return &pipeline, nil
}

func fileHasSuffix(arr []string, str string) bool {
	for _, a := range arr {
		if strings.HasSuffix(str, a) {
			return true
		}
	}
	return false
}

func (b *builder) CreateTaskFromFile(path string) (*Task, error) {
	isSeparateDefinitionFile := false
	creator := b.commentTaskCreator

	if fileHasSuffix(b.config.TasksFileSuffixes, path) {
		creator = b.yamlTaskCreator
		isSeparateDefinitionFile = true
	}

	task, err := creator(path)
	if err != nil {
		return nil, errors.Wrapf(err, "error creating task from file '%s'", path)
	}

	if task == nil {
		return nil, nil
	}

	task.DefinitionFile.Name = filepath.Base(path)
	task.DefinitionFile.Path = path
	task.DefinitionFile.Type = CommentTask
	if isSeparateDefinitionFile {
		task.DefinitionFile.Type = YamlTask
	}

	return task, nil
}
