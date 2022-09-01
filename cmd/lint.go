package cmd

import (
	"github.com/datablast-analytics/blast-cli/pkg/lint"
	"github.com/datablast-analytics/blast-cli/pkg/path"
	"github.com/datablast-analytics/blast-cli/pkg/pipeline"
	"github.com/urfave/cli/v2"
)

func Lint(isDebug *bool) *cli.Command {
	return &cli.Command{
		Name:      "validate",
		Usage:     "validate the blast pipeline configuration for all the pipelines in a given directory",
		ArgsUsage: "[path to pipelines]",
		Action: func(c *cli.Context) error {
			logger := makeLogger(*isDebug)

			builderConfig := pipeline.BuilderConfig{
				PipelineFileName:   pipelineDefinitionFile,
				TasksDirectoryName: defaultTasksPath,
				TasksFileSuffixes:  defaultTaskFileSuffixes,
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
	}
}
