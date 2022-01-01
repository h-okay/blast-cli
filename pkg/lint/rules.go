package lint

import (
	"fmt"
	"os"

	"github.com/datablast-analytics/blast-cli/pkg/pipeline"
	"github.com/pkg/errors"
	"github.com/robfig/cron/v3"
	"github.com/spf13/afero"
)

const (
	nameExistsDescription = `A task must have a name`

	executableFileDoesNotExist    = `The executable file does not exist`
	executableFileIsADirectory    = `The executable file is a directory, must be a file`
	executableFileIsEmpty         = `The executable file is empty`
	executableFileIsNotExecutable = "Executable file is not executable, give it the '644' or '755' permissions"
)

var validTaskTypes = map[string]struct{}{
	"bq.sql":               {},
	"bq.sensor.table":      {},
	"bash":                 {},
	"gcs.from.s3":          {},
	"python":               {},
	"s3.sensor.key_sensor": {},
	"sf.sql":               {},
}

func EnsureNameExists(pipeline *pipeline.Pipeline) ([]*Issue, error) {
	issues := make([]*Issue, 0)
	for _, task := range pipeline.Tasks {
		if task.Name == "" {
			issues = append(issues, &Issue{
				Task:        task,
				Description: nameExistsDescription,
			})
		}
	}

	return issues, nil
}

func EnsureExecutableFileIsValid(fs afero.Fs) PipelineValidator {
	return func(p *pipeline.Pipeline) ([]*Issue, error) {
		issues := make([]*Issue, 0)
		for _, task := range p.Tasks {
			if task.DefinitionFile.Type == pipeline.CommentTask {
				continue
			}

			if task.ExecutableFile.Path == "" {
				continue
			}

			fileInfo, err := fs.Stat(task.ExecutableFile.Path)
			if errors.Is(err, os.ErrNotExist) {
				issues = append(issues, &Issue{
					Task:        task,
					Description: executableFileDoesNotExist,
				})
				continue
			}

			if fileInfo.IsDir() {
				issues = append(issues, &Issue{
					Task:        task,
					Description: executableFileIsADirectory,
				})
				continue
			}

			if fileInfo.Size() == 0 {
				issues = append(issues, &Issue{
					Task:        task,
					Description: executableFileIsEmpty,
				})
			}

			if fileInfo.Mode().Perm() != 0o755 && fileInfo.Mode().Perm() != 0o644 {
				issues = append(issues, &Issue{
					Task:        task,
					Description: executableFileIsNotExecutable,
				})
			}
		}

		return issues, nil
	}
}

func EnsureDependencyExists(p *pipeline.Pipeline) ([]*Issue, error) {
	taskMap := map[string]bool{}
	for _, task := range p.Tasks {
		if task.Name == "" {
			continue
		}

		taskMap[task.Name] = true
	}

	issues := make([]*Issue, 0)
	for _, task := range p.Tasks {
		for _, dep := range task.DependsOn {
			if _, ok := taskMap[dep]; !ok {
				issues = append(issues, &Issue{
					Task:        task,
					Description: fmt.Sprintf("Dependency '%s' does not exist", dep),
				})
			}
		}
	}

	return issues, nil
}

func EnsurePipelineScheduleIsValidCron(p *pipeline.Pipeline) ([]*Issue, error) {
	issues := make([]*Issue, 0)
	if p.Schedule == "" {
		return issues, nil
	}

	_, err := cron.ParseStandard(string(p.Schedule))
	if err != nil {
		issues = append(issues, &Issue{
			Description: fmt.Sprintf("Invalid cron schedule '%s'", p.Schedule),
		})
	}

	return issues, nil
}

func EnsureOnlyAcceptedTaskTypesAreThere(p *pipeline.Pipeline) ([]*Issue, error) {
	issues := make([]*Issue, 0)

	for _, task := range p.Tasks {
		if task.Type == "" {
			continue
		}

		if _, ok := validTaskTypes[task.Type]; !ok {
			issues = append(issues, &Issue{
				Task:        task,
				Description: fmt.Sprintf("Invalid task type '%s'", task.Type),
			})
		}
	}

	return issues, nil
}
