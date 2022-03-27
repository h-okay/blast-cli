package main

import (
	"os"
	"time"

	"github.com/datablast-analytics/blast-cli/pkg/lint"
	"github.com/datablast-analytics/blast-cli/pkg/path"
	"github.com/datablast-analytics/blast-cli/pkg/pipeline"
	"github.com/fatih/color"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
)

const (
	defaultPipelinePath    = "."
	pipelineDefinitionFile = "pipeline.yml"
	defaultTasksPath       = "tasks"
	defaultTaskFileName    = "task.yml"
)

func main() {
	isDebug := false
	color.NoColor = false

	app := &cli.App{
		Name:     "blast",
		Usage:    "The CLI used for managing Blast-powered data pipelines",
		Compiled: time.Now(),
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:        "debug",
				Value:       false,
				Usage:       "show debug information",
				Destination: &isDebug,
			},
		},
		Commands: []*cli.Command{
			{
				Name:      "validate",
				Usage:     "validate the blast pipeline configuration for all the pipelines in a given directory",
				ArgsUsage: "[path to pipelines]",
				Action: func(c *cli.Context) error {
					errorPrinter := color.New(color.FgRed, color.Bold)
					logger := makeLogger(isDebug)

					builderConfig := pipeline.BuilderConfig{
						PipelineFileName:   pipelineDefinitionFile,
						TasksDirectoryName: defaultTasksPath,
						TasksFileName:      defaultTaskFileName,
					}
					builder := pipeline.NewBuilder(builderConfig, pipeline.CreateTaskFromYamlDefinition, pipeline.CreateTaskFromFileComments)

					rules, err := lint.GetRules(logger)
					if err != nil {
						errorPrinter.Printf("An error occurred while linting the pipelines: %v\n", err)
						return cli.Exit("", 1)
					}

					linter := lint.NewLinter(path.GetPipelinePaths, builder, rules, logger)

					rootPath := c.Args().Get(0)
					if rootPath == "" {
						rootPath = defaultPipelinePath
					}

					result, err := linter.Lint(rootPath, pipelineDefinitionFile)
					if err != nil {
						errorPrinter.Printf("An error occurred while linting the pipelines: %v\n", err)
						return cli.Exit("", 1)
					}

					printer := lint.Printer{
						RootCheckPath: rootPath,
					}
					printer.PrintIssues(result)

					if result.HasErrors() {
						return cli.Exit("", 1)
					}

					return nil
				},
			},
		},
	}

	_ = app.Run(os.Args)
}

func makeLogger(isDebug bool) *zap.SugaredLogger {
	config := zap.NewProductionConfig()
	if isDebug {
		config = zap.NewDevelopmentConfig()
	}

	config.Sampling = nil
	config.Encoding = "console"

	logger, err := config.Build()
	if err != nil {
		panic(err)
	}

	return logger.Sugar()
}
