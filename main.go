package main

import (
	"os"
	"time"

	"github.com/datablast-analytics/blast-cli/pkg/lint"
	"github.com/datablast-analytics/blast-cli/pkg/path"
	"github.com/datablast-analytics/blast-cli/pkg/pipeline"
	"github.com/fatih/color"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
)

const (
	defaultPipelinePath    = "."
	pipelineDefinitionFile = "pipeline.yml"
	defaultTasksPath       = "tasks"
	defaultTaskFileName    = "task.yml"
)

var validationRules = []*lint.Rule{
	{
		Name:    "task-name-exists",
		Checker: lint.EnsureTaskNameIsNotEmpty,
	},
	{
		Name:    "task-name-unique",
		Checker: lint.EnsureTaskNameIsUnique,
	},
	{
		Name:    "dependency-exists",
		Checker: lint.EnsureDependencyExists,
	},
	{
		Name:    "valid-executable-file",
		Checker: lint.EnsureExecutableFileIsValid(afero.NewCacheOnReadFs(afero.NewOsFs(), afero.NewMemMapFs(), 100*time.Second)),
	},
	{
		Name:    "valid-pipeline-schedule",
		Checker: lint.EnsurePipelineScheduleIsValidCron,
	},
	{
		Name:    "valid-pipeline-name",
		Checker: lint.EnsurePipelineNameIsValid,
	},
	{
		Name:    "valid-task-type",
		Checker: lint.EnsureOnlyAcceptedTaskTypesAreThere,
	},
}

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
					builderConfig := pipeline.BuilderConfig{
						PipelineFileName:   pipelineDefinitionFile,
						TasksDirectoryName: defaultTasksPath,
						TasksFileName:      defaultTaskFileName,
					}
					builder := pipeline.NewBuilder(builderConfig, pipeline.CreateTaskFromYamlDefinition, pipeline.CreateTaskFromFileComments)
					linter := lint.NewLinter(path.GetPipelinePaths, builder, validationRules, makeLogger(isDebug))

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

					printer := lint.Printer{}
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
	logger, err := zap.NewProduction()
	if isDebug {
		logger, err = zap.NewDevelopment()
	}

	if err != nil {
		panic(err)
	}

	return logger.Sugar()
}
