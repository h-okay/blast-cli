package lint

import (
	"os"

	"github.com/datablast-analytics/blast-cli/pkg/pipeline"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

const (
	nameExistsDescription = `A task must have a name`

	executableFileDoesNotExist    = `The executable file does not exist`
	executableFileIsADirectory    = `The executable file is a directory, must be a file`
	executableFileIsEmpty         = `The executable file is empty`
	executableFileIsNotExecutable = "Executable file is not executable, give it the '644' or '755' permissions"
)

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
