package pipeline

import (
	"path/filepath"
	"strings"

	"github.com/datablast-analytics/blast-cli/pkg/path"
	"github.com/pkg/errors"
)

const (
	CommentTask TaskDefinitionType = "comment"
	YamlTask    TaskDefinitionType = "yaml"
)

type (
	schedule           string
	TaskDefinitionType string
)

type ExecutableFile struct {
	Name string
	Path string
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

type Task struct {
	Name           string
	Description    string
	Type           string
	ExecutableFile ExecutableFile
	DefinitionFile TaskDefinitionFile
	Parameters     map[string]string
	Connections    map[string]string
	DependsOn      []string
	Pipeline       *Pipeline
	Schedule       TaskSchedule
}

type Pipeline struct {
	LegacyID           string   `yaml:"id"`
	Name               string   `yaml:"name"`
	Schedule           schedule `yaml:"schedule"`
	DefinitionFile     DefinitionFile
	DefaultParameters  map[string]string `yaml:"defaultParameters"`
	DefaultConnections map[string]string `yaml:"defaultConnections"`
	Tasks              []*Task
	Notifications      Notifications `yaml:"notifications"`

	tasksByType map[string][]*Task
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
	_, ok := p.tasksByType[taskType]
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
}

func NewBuilder(config BuilderConfig, yamlTaskCreator TaskCreator, commentTaskCreator TaskCreator) *builder {
	return &builder{
		config:             config,
		yamlTaskCreator:    yamlTaskCreator,
		commentTaskCreator: commentTaskCreator,
	}
}

func (b *builder) CreatePipelineFromPath(pathToPipeline string) (*Pipeline, error) {
	pipelineFilePath := filepath.Join(pathToPipeline, b.config.PipelineFileName)
	tasksPath := filepath.Join(pathToPipeline, b.config.TasksDirectoryName)

	var pipeline Pipeline
	err := path.ReadYaml(pipelineFilePath, &pipeline)
	if err != nil {
		return nil, errors.Wrapf(err, "error reading pipeline file at '%s'", pipelineFilePath)
	}

	// this is needed until we migrate all the pipelines to use the new naming convention
	if pipeline.Name == "" {
		pipeline.Name = pipeline.LegacyID
	}
	pipeline.tasksByType = make(map[string][]*Task)
	pipeline.tasksByName = make(map[string]*Task)

	absPipelineFilePath, err := filepath.Abs(pipelineFilePath)
	if err != nil {
		return nil, errors.Wrapf(err, "error getting absolute path for pipeline file at '%s'", pipelineFilePath)
	}

	pipeline.DefinitionFile = DefinitionFile{
		Name: filepath.Base(pipelineFilePath),
		Path: absPipelineFilePath,
	}

	taskFiles, err := path.GetAllFilesRecursive(tasksPath)
	if err != nil {
		return nil, errors.Wrapf(err, "error listing Task files at '%s'", tasksPath)
	}

	for _, file := range taskFiles {
		task, err := b.CreateTaskFromFile(file)
		if err != nil {
			return nil, errors.Wrapf(err, "error creating Task from file '%s'", file)
		}

		if task == nil {
			continue
		}

		pipeline.Tasks = append(pipeline.Tasks, task)

		if _, ok := pipeline.tasksByType[task.Type]; !ok {
			pipeline.tasksByType[task.Type] = make([]*Task, 0)
		}

		pipeline.tasksByType[task.Type] = append(pipeline.tasksByType[task.Type], task)
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

func (p *builder) CreateTaskFromFile(path string) (*Task, error) {
	isSeparateDefinitionFile := false
	creator := p.commentTaskCreator

	if fileHasSuffix(p.config.TasksFileSuffixes, path) {
		creator = p.yamlTaskCreator
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
