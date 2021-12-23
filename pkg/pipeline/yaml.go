package pipeline

import (
	"github.com/datablast-analytics/blast-cli/pkg/path"
	"github.com/pkg/errors"
	"path/filepath"
)

type taskDefinition struct {
	Name        string            `yaml:"name" validate:"required,min=1"`
	Description string            `yaml:"description"`
	Type        string            `yaml:"type" validate:"required,min=1"`
	RunFile     string            `yaml:"run" validate:"min=1"`
	Depends     []string          `yaml:"depends"`
	Parameters  map[string]string `yaml:"parameters"`
	Connections map[string]string `yaml:"connections"`
}

func CreateTaskFromYamlDefinition(filePath string) (*Task, error) {
	filePath, err := filepath.Abs(filePath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get absolute path for the definition file")
	}

	var definition taskDefinition
	err = path.ReadYaml(filePath, &definition)
	if err != nil {
		return nil, errors.Wrap(err, "unable to read the task definition file")
	}

	relativeRunFilePath := filepath.Join(filepath.Dir(filePath), definition.RunFile)
	absRunFile, err := filepath.Abs(relativeRunFilePath)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to resolve the absolute run file path: %s", definition.RunFile)
	}

	task := Task{
		Name:        definition.Name,
		Description: definition.Description,
		Type:        definition.Type,
		Parameters:  definition.Parameters,
		Connections: definition.Connections,
		DependsOn:   definition.Depends,
		ExecutableFile: ExecutableFile{
			Name: filepath.Base(definition.RunFile),
			Path: absRunFile,
		},
	}

	return &task, nil
}
