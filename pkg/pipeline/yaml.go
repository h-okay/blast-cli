package pipeline

import (
	"path/filepath"

	"github.com/datablast-analytics/blast-cli/pkg/path"
	"github.com/pkg/errors"
)

type taskDefinition struct {
	Name        string            `yaml:"name"`
	Description string            `yaml:"description"`
	Type        string            `yaml:"type"`
	RunFile     string            `yaml:"run"`
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

	executableFile := ExecutableFile{}
	if definition.RunFile != "" {

		relativeRunFilePath := filepath.Join(filepath.Dir(filePath), definition.RunFile)
		absRunFile, err := filepath.Abs(relativeRunFilePath)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to resolve the absolute run file path: %s", definition.RunFile)
		}

		executableFile.Name = filepath.Base(definition.RunFile)
		executableFile.Path = absRunFile
	}

	task := Task{
		Name:           definition.Name,
		Description:    definition.Description,
		Type:           definition.Type,
		Parameters:     definition.Parameters,
		Connections:    definition.Connections,
		DependsOn:      definition.Depends,
		ExecutableFile: executableFile,
	}

	return &task, nil
}
