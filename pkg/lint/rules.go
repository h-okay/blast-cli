package lint

import "github.com/datablast-analytics/blast-cli/pkg/pipeline"

const (
	nameExistsDescription = `A task must have a name`
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
