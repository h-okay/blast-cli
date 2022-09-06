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

			rules, err := lint.GetRules(logger, fs)
			if err != nil {
				errorPrinter.Printf("An error occurred while linting the pipelines: %v\n", err)
				return cli.Exit("", 1)
			}

			builderConfig := pipeline.BuilderConfig{
				PipelineFileName:   pipelineDefinitionFile,
				TasksDirectoryName: defaultTasksPath,
				TasksFileSuffixes:  defaultTaskFileSuffixes,
			}
			builder := pipeline.NewBuilder(builderConfig, pipeline.CreateTaskFromYamlDefinition(fs), pipeline.CreateTaskFromFileComments(fs), fs)
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

			// prepare the final message
			errorCount := result.ErrorCount()
			pipelineCount := len(result.Pipelines)
			pipelineStr := "pipeline"
			if pipelineCount > 1 {
				pipelineStr += "s"
			}

			if errorCount > 0 {
				issueStr := "issue"
				if errorCount > 1 {
					issueStr += "s"
				}

				errorPrinter.Printf("\n✘ Checked %d %s and found %d %s, please check above.\n", pipelineCount, pipelineStr, errorCount, issueStr)
				return cli.Exit("", 1)
			} else {
				taskCount := 0
				for _, p := range result.Pipelines {
					taskCount += len(p.Pipeline.Tasks)
				}

				successPrinter.Printf("\n✓ Successfully validated %d tasks across %d %s, all good.\n", taskCount, pipelineCount, pipelineStr)
			}

			return nil
		},
	}
}
