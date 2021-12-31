package main

import (
	"github.com/datablast-analytics/blast-cli/pkg/lint"
	"github.com/datablast-analytics/blast-cli/pkg/path"
	"github.com/datablast-analytics/blast-cli/pkg/pipeline"
	"github.com/fatih/color"
	"github.com/spf13/afero"
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
		{
			Name:        "valid-executable-file",
			Description: "",
			Checker:     lint.EnsureExecutableFileIsValid(afero.NewCacheOnReadFs(afero.NewOsFs(), afero.NewMemMapFs(), 100*time.Second)),
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

					printer := lint.Printer{}
					printer.PrintIssues(result)

					return nil
				},
			},
		},
	}

	_ = app.Run(os.Args)
}
