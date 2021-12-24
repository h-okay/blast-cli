package pipeline

import (
	"path/filepath"
	"strings"

	"github.com/datablast-analytics/blast-cli/pkg/path"
	"github.com/pkg/errors"
)

const (
	pipelineFileName = "pipeline.yml"
	taskFileName     = "task.yml"
)

type schedule string

type ExecutableFile struct {
	Name string
	Path string
}

type Task struct {
	Name           string
	Description    string
	Type           string
	ExecutableFile ExecutableFile
	Parameters     map[string]string
	Connections    map[string]string
	DependsOn      []string
}

type Pipeline struct {
	Name               string            `yaml:"name" validate:"required"`
	Schedule           schedule          `yaml:"schedule"`
	DefaultParameters  map[string]string `yaml:"defaultParameters"`
	DefaultConnections map[string]string `yaml:"defaultConnections"`
	Tasks              []*Task
}

type TaskCreator func(path string) (*Task, error)

type builder struct {
	tasksDirectoryName string
	yamlTaskCreator    TaskCreator
	commentTaskCreator TaskCreator
}

func NewBuilder(tasksDirectoryName string, yamlTaskCreator TaskCreator, commentTaskCreator TaskCreator) *builder {
	return &builder{
		tasksDirectoryName: tasksDirectoryName,
		yamlTaskCreator:    yamlTaskCreator,
		commentTaskCreator: commentTaskCreator,
	}
}

func (p *builder) CreatePipelineFromPath(pathToPipeline string) (*Pipeline, error) {
	pipelineFilePath := filepath.Join(pathToPipeline, pipelineFileName)
	tasksPath := filepath.Join(pathToPipeline, p.tasksDirectoryName)

	var pipeline Pipeline
	err := path.ReadYaml(pipelineFilePath, &pipeline)
	if err != nil {
		return nil, errors.Wrapf(err, "error reading pipeline file at '%s'", pipelineFilePath)
	}

	taskFiles, err := path.GetAllFilesRecursive(tasksPath)
	if err != nil {
		return nil, errors.Wrapf(err, "error listing Task files at '%s'", tasksPath)
	}

	taskFiles = path.ExcludeSubItemsInDirectoryContainingFile(taskFiles, taskFileName)
	for _, file := range taskFiles {
		creator := p.commentTaskCreator
		if strings.HasSuffix(file, taskFileName) {
			creator = p.yamlTaskCreator
		}

		task, err := creator(file)
		if err != nil {
			return nil, errors.Wrapf(err, "error creating Task from file '%s'", file)
		}

		if task == nil {
			continue
		}

		pipeline.Tasks = append(pipeline.Tasks, task)
	}

	return &pipeline, nil
}
