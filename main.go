package main

import (
	"github.com/datablast-analytics/blast-cli/pkg/lint"
	"github.com/datablast-analytics/blast-cli/pkg/path"
	"log"
	"os"

	"github.com/urfave/cli/v2"
)

const (
	defaultPipelinePath    = "."
	pipelineDefinitionFile = "pipeline.yml"
)

func main() {
	linter := lint.NewLinter(path.GetPipelinePaths, []lint.Rule{})
	app := &cli.App{
		Commands: []*cli.Command{
			{
				Name:    "lint",
				Aliases: []string{"a"},
				Usage:   "lint the blast pipeline configuration",
				Action: func(c *cli.Context) error {
					rootPath := c.Args().Get(0)
					if rootPath == "" {
						rootPath = defaultPipelinePath
					}

					return linter.Lint(rootPath, pipelineDefinitionFile)
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
