package main

import (
	"fmt"
	"github.com/datablast-analytics/blast-cli/pkg/lint"
	"github.com/datablast-analytics/blast-cli/pkg/path"
	"github.com/datablast-analytics/blast-cli/pkg/pipeline"
	"github.com/fatih/color"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
	"os"
	"time"
)

const (
	defaultPipelinePath    = "."
	pipelineDefinitionFile = "pipeline.yml"
	defaultTasksPath       = "tasks"
	defaultTaskFileName    = "task.yml"
)

var (
	validationRules = []*lint.Rule{
		{
			Name:        "name-exists",
			Description: "",
			Checker:     lint.EnsureNameExists,
		},
	}
)

func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}

	defer logger.Sync() // flushes buffer, if any
	sugaredLogger := logger.Sugar()

	app := &cli.App{
		Name:     "blast",
		Usage:    "The CLI used for managing Blast-powered data pipelines",
		Compiled: time.Now(),
		Commands: []*cli.Command{
			{
				Name:      "validate",
				Usage:     "validate the blast pipeline configuration for all the pipelines in a given directory",
				ArgsUsage: "[path to pipelines]",
				Action: func(c *cli.Context) error {
					builderConfig := pipeline.BuilderConfig{
						PipelineFileName:   pipelineDefinitionFile,
						TasksDirectoryName: defaultTasksPath,
						TasksFileName:      defaultTaskFileName,
					}
					builder := pipeline.NewBuilder(builderConfig, pipeline.CreateTaskFromYamlDefinition, pipeline.CreateTaskFromFileComments)
					linter := lint.NewLinter(path.GetPipelinePaths, builder, validationRules, sugaredLogger)

					rootPath := c.Args().Get(0)
					if rootPath == "" {
						rootPath = defaultPipelinePath
					}
					result, err := linter.Lint(rootPath, pipelineDefinitionFile)

					if err != nil {
						printer := color.New(color.FgRed, color.Bold)
						printer.Printf("An error occurred while linting the pipelines: %v\n", err)
						return cli.Exit("", 1)
					}

					successPrinter := color.New(color.FgGreen, color.Bold)

					for _, pipeline := range result.Issues {
						fmt.Println()
						issuePrinter := color.New(color.FgRed, color.Bold)

						color.Yellow("Pipeline: %s", pipeline.Pipeline.Name)

						if len(pipeline.Issues) == 0 {
							successPrinter.Println("  No issues found")
							continue
						}

						for rule, issues := range pipeline.Issues {
							for _, issue := range issues {
								issuePrinter.Printf("  %s: %s - %s\n", rule.Name, issue.Task.Name, issue.Description)
							}
						}

					}




					return nil
				},
			},
		},
	}

	_ = app.Run(os.Args)
}
