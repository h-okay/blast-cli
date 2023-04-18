package cmd

import (
	"fmt"
	path2 "path"
	"strings"

	"github.com/datablast-analytics/blast/pkg/config"
	"github.com/datablast-analytics/blast/pkg/connection"
	"github.com/datablast-analytics/blast/pkg/executor"
	"github.com/datablast-analytics/blast/pkg/lint"
	"github.com/datablast-analytics/blast/pkg/path"
	"github.com/datablast-analytics/blast/pkg/query"
	"github.com/manifoldco/promptui"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v2"
)

func Lint(isDebug *bool) *cli.Command {
	return &cli.Command{
		Name:      "validate",
		Usage:     "validate the blast pipeline configuration for all the pipelines in a given directory",
		ArgsUsage: "[path to pipelines]",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "environment",
				Aliases: []string{"e"},
				Usage:   "the environment to use",
			},
			&cli.BoolFlag{
				Name:    "force",
				Aliases: []string{"f"},
				Usage:   "force the validation even if the environment is a production environment",
			},
		},
		Action: func(c *cli.Context) error {
			fmt.Println()

			logger := makeLogger(*isDebug)

			rootPath := c.Args().Get(0)
			if rootPath == "" {
				rootPath = "."
			}

			cm, err := config.LoadOrCreate(afero.NewOsFs(), path2.Join(rootPath, ".blast.yml"))
			if err != nil {
				errorPrinter.Printf("Failed to load the config file: %v\n", err)
				return cli.Exit("", 1)
			}

			env := c.String("environment")
			if env != "" {
				err = cm.SelectEnvironment(env)
				if err != nil {
					errorPrinter.Printf("Failed to use the environment '%s': %v\n", env, err)
					return cli.Exit("", 1)
				}

				// if env name is similar to "prod" ask for confirmation
				if !c.Bool("force") && strings.Contains(strings.ToLower(env), "prod") {
					prompt := promptui.Prompt{
						Label:     "You are using a production environment. Are you sure you want to continue?",
						IsConfirm: true,
					}

					_, err := prompt.Run()
					if err != nil {
						fmt.Printf("The operation is cancelled.\n")
						return cli.Exit("", 1)
					}
				}
			}

			connectionManager, err := connection.NewManagerFromConfig(cm)
			if err != nil {
				errorPrinter.Printf("Failed to register connections: %v\n", err)
				return cli.Exit("", 1)
			}

			rules, err := lint.GetRules(logger, fs)
			if err != nil {
				errorPrinter.Printf("An error occurred while building the validation rules: %v\n", err)
				return cli.Exit("", 1)
			}

			if len(cm.SelectedEnvironment.Connections.GoogleCloudPlatform) > 0 {
				rules = append(rules, &lint.QueryValidatorRule{
					Identifier:  "bigquery-validator",
					TaskType:    executor.TaskTypeBigqueryQuery,
					Connections: connectionManager,
					Extractor: &query.WholeFileExtractor{
						Fs:       fs,
						Renderer: query.DefaultJinjaRenderer,
					},
					WorkerCount: 32,
					Logger:      logger,
				})
			}

			linter := lint.NewLinter(path.GetPipelinePaths, builder, rules, logger)

			infoPrinter.Printf("Validating pipelines in '%s' for '%s' environment...\n", rootPath, cm.SelectedEnvironmentName)
			result, err := linter.Lint(rootPath, pipelineDefinitionFile)

			printer := lint.Printer{RootCheckPath: rootPath}
			err = reportLintErrors(result, err, printer)
			if err != nil {
				return cli.Exit("", 1)
			}
			return nil
		},
	}
}

func reportLintErrors(result *lint.PipelineAnalysisResult, err error, printer lint.Printer) error {
	if err != nil {
		errorPrinter.Println("\nAn error occurred while linting the pipelines:")

		errorList := unwrapAllErrors(err)
		for i, e := range errorList {
			errorPrinter.Printf("%s└── %s\n", strings.Repeat("  ", i), e)
		}

		return err
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
		return errors.New("validation failed")
	}

	taskCount := 0
	for _, p := range result.Pipelines {
		taskCount += len(p.Pipeline.Tasks)
	}

	successPrinter.Printf("\n✓ Successfully validated %d tasks across %d %s, all good.\n", taskCount, pipelineCount, pipelineStr)
	return nil
}

func unwrapAllErrors(err error) []string {
	if err == nil {
		return []string{}
	}

	errorItems := flattenErrors(err)
	count := len(errorItems)
	if count < 2 {
		return errorItems
	}

	cleanErrors := make([]string, count)
	cleanErrors[count-1] = errorItems[0]
	for i := range errorItems {
		if i == count-1 {
			break
		}

		rev := count - i - 1
		item := errorItems[rev]

		cleanMessage := strings.ReplaceAll(item, ": "+errorItems[rev-1], "")
		cleanErrors[i] = cleanMessage
	}

	return cleanErrors
}

func flattenErrors(err error) []string {
	if err == nil {
		return []string{}
	}

	unwrapped := errors.Unwrap(err)
	if unwrapped == nil {
		return []string{err.Error()}
	}

	for unwrapped != nil && err.Error() == unwrapped.Error() {
		unwrapped = errors.Unwrap(unwrapped)
	}

	var foundErrors []string
	allErrors := flattenErrors(unwrapped)
	foundErrors = append(foundErrors, allErrors...)
	foundErrors = append(foundErrors, err.Error())

	return foundErrors
}
