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

type DefinitionFile struct {
	Name string
	Path string
	Type TaskDefinitionType
}

type Task struct {
	Name           string
	Description    string
	Type           string
	ExecutableFile ExecutableFile
	DefinitionFile DefinitionFile
	Parameters     map[string]string
	Connections    map[string]string
	DependsOn      []string
	Pipeline       *Pipeline
}

type Pipeline struct {
	LegacyID           string   `yaml:"id"`
	Name               string   `yaml:"name"`
	Schedule           schedule `yaml:"schedule"`
	DefinitionFile     DefinitionFile
	DefaultParameters  map[string]string `yaml:"defaultParameters"`
	DefaultConnections map[string]string `yaml:"defaultConnections"`
	Tasks              []*Task
}

func (p *Pipeline) RelativeTaskPath(t *Task) string {
	absolutePipelineRoot := filepath.Dir(p.DefinitionFile.Path)

	pipelineDirectory, err := filepath.Rel(absolutePipelineRoot, t.DefinitionFile.Path)
	if err != nil {
		return absolutePipelineRoot
	}

	return pipelineDirectory
}

type TaskCreator func(path string) (*Task, error)

type BuilderConfig struct {
	PipelineFileName   string
	TasksDirectoryName string
	TasksFileName      string
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

func (p *builder) CreatePipelineFromPath(pathToPipeline string) (*Pipeline, error) {
	pipelineFilePath := filepath.Join(pathToPipeline, p.config.PipelineFileName)
	tasksPath := filepath.Join(pathToPipeline, p.config.TasksDirectoryName)

	var pipeline Pipeline
	err := path.ReadYaml(pipelineFilePath, &pipeline)
	if err != nil {
		return nil, errors.Wrapf(err, "error reading pipeline file at '%s'", pipelineFilePath)
	}

	// this is needed until we migrate all the pipelines to use the new naming convention
	if pipeline.Name == "" {
		pipeline.Name = pipeline.LegacyID
	}

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
		isSeparateDefinitionFile := false
		creator := p.commentTaskCreator
		if strings.HasSuffix(file, p.config.TasksFileName) {
			creator = p.yamlTaskCreator
			isSeparateDefinitionFile = true
		}

		task, err := creator(file)
		if err != nil {
			return nil, errors.Wrapf(err, "error creating Task from file '%s'", file)
		}

		if task == nil {
			continue
		}

		task.DefinitionFile.Name = filepath.Base(file)
		task.DefinitionFile.Path = file
		task.DefinitionFile.Type = CommentTask
		if isSeparateDefinitionFile {
			task.DefinitionFile.Type = YamlTask
		}

		pipeline.Tasks = append(pipeline.Tasks, task)
	}

	return &pipeline, nil
}
