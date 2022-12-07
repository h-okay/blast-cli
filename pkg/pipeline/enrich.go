package pipeline

import (
	"github.com/datablast-analytics/blast-cli/pkg/query"
	"github.com/flosch/pongo2/v6"
	"strings"
)

func TemplatedEnricher(creator TaskCreator) TaskCreator {
	return func(filePath string) (*Task, error) {
		task, err := creator(filePath)
		if err != nil {
			return nil, err
		}

		if !strings.HasSuffix(task.ExecutableFile.Path, ".sql") {
			return task, nil
		}

		refs := make([]string, 0)
		context := pongo2.Context{
			"ref": func(name string) string {
				refs = append(refs, name)
			},
		}

		renderer := query.NewJinjaRenderer(context)
		renderer.Render(task.ExecutableFile.Content)

		return task, nil
	}
}
