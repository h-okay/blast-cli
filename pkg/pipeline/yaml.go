package pipeline

import (
	"path/filepath"
	"strings"

	"github.com/datablast-analytics/blast-cli/pkg/path"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"gopkg.in/yaml.v3"
)

type taskSchedule struct {
	Days []string `yaml:"days"`
}

func mustBeStringArray(fieldName string, value *yaml.Node) ([]string, error) {
	var multi []string
	err := value.Decode(&multi)
	if err != nil {
		return nil, errors.New("`" + fieldName + "` field must be an array of strings")
	}
	return multi, nil
}

type depends []string

func (a *depends) UnmarshalYAML(value *yaml.Node) error {
	multi, err := mustBeStringArray("depends", value)
	*a = multi
	return err
}

type clusterBy []string

func (a *clusterBy) UnmarshalYAML(value *yaml.Node) error {
	multi, err := mustBeStringArray("cluster_by", value)
	*a = multi
	return err
}

type materialization struct {
	Type           string    `yaml:"type"`
	Strategy       string    `yaml:"strategy"`
	PartitionBy    string    `yaml:"partition_by"`
	ClusterBy      clusterBy `yaml:"cluster_by"`
	IncrementalKey string    `yaml:"incremental_key"`
}

type taskDefinition struct {
	Name            string            `yaml:"name"`
	Description     string            `yaml:"description"`
	Type            string            `yaml:"type"`
	RunFile         string            `yaml:"run"`
	Depends         depends           `yaml:"depends"`
	Parameters      map[string]string `yaml:"parameters"`
	Connections     map[string]string `yaml:"connections"`
	Schedule        taskSchedule      `yaml:"schedule"`
	Materialization materialization   `yaml:"materialization"`
}

func CreateTaskFromYamlDefinition(fs afero.Fs) TaskCreator {
	return func(filePath string) (*Task, error) {
		filePath, err := filepath.Abs(filePath)
		if err != nil {
			return nil, errors.Wrap(err, "failed to get absolute path for the definition file")
		}

		var definition taskDefinition
		err = path.ReadYaml(fs, filePath, &definition)
		if err != nil {
			return nil, err
		}

		buf, err := afero.ReadFile(fs, filePath)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to read file %s", filePath)
		}

		task, err := ConvertYamlToTask(buf)
		if err != nil {
			return nil, err
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

			content, err := afero.ReadFile(fs, absRunFile)
			if err != nil {
				return nil, errors.Wrapf(err, "unable to read the run file: %s", absRunFile)
			}
			executableFile.Content = string(content)
		}
		task.ExecutableFile = executableFile

		return task, nil
	}
}

func ConvertYamlToTask(content []byte) (*Task, error) {
	var definition taskDefinition
	err := path.ConvertYamlToObject(content, &definition)
	if err != nil {
		return nil, err
	}

	mat := Materialization{
		Type:           MaterializationType(strings.ToLower(definition.Materialization.Type)),
		Strategy:       MaterializationStrategy(strings.ToLower(definition.Materialization.Strategy)),
		ClusterBy:      definition.Materialization.ClusterBy,
		PartitionBy:    definition.Materialization.PartitionBy,
		IncrementalKey: definition.Materialization.IncrementalKey,
	}

	task := Task{
		Name:            definition.Name,
		Description:     definition.Description,
		Type:            definition.Type,
		Parameters:      definition.Parameters,
		Connections:     definition.Connections,
		DependsOn:       definition.Depends,
		ExecutableFile:  ExecutableFile{},
		Schedule:        TaskSchedule{Days: definition.Schedule.Days},
		Materialization: mat,
	}

	return &task, nil
}
