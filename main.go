package main

import (
	"github.com/datablast-analytics/blast-cli/pkg/lint"
	"github.com/datablast-analytics/blast-cli/pkg/path"
	"github.com/datablast-analytics/blast-cli/pkg/pipeline"
	"github.com/fatih/color"
	"github.com/urfave/cli/v2"
	"os"
)

const (
	defaultPipelinePath    = "."
	pipelineDefinitionFile = "pipeline.yml"
	defaultTasksPath       = "tasks"
)

func main() {
	builder := pipeline.NewBuilder(defaultTasksPath, pipeline.CreateTaskFromYamlDefinition, pipeline.CreateTaskFromFileComments)
	linter := lint.NewLinter(path.GetPipelinePaths, builder, []lint.Rule{})

	app := &cli.App{
		Commands: []*cli.Command{
			{
				Name:    "lint",
				Aliases: []string{"l"},
				Usage:   "lint the blast pipeline configuration",
				Action: func(c *cli.Context) error {
					rootPath := c.Args().Get(0)
					if rootPath == "" {
						rootPath = defaultPipelinePath
					}

					err := linter.Lint(rootPath, pipelineDefinitionFile)

					if err != nil {
						printer := color.New(color.FgRed, color.Bold)
						printer.Printf("An error occurred while linting the pipelines: %v\n", err)
						return cli.Exit("", 1)
					}

					printer := color.New(color.FgGreen, color.Bold)
					printer.Println("The pipelines have successfully been linted.")

					return nil
				},
			},
		},
	}

	_ = app.Run(os.Args)
}
