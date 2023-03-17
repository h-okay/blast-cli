package cmd

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/datablast-analytics/blast-cli/pkg/bigquery"
	"github.com/datablast-analytics/blast-cli/pkg/date"
	"github.com/datablast-analytics/blast-cli/pkg/executor"
	"github.com/datablast-analytics/blast-cli/pkg/lint"
	"github.com/datablast-analytics/blast-cli/pkg/path"
	"github.com/datablast-analytics/blast-cli/pkg/pipeline"
	"github.com/datablast-analytics/blast-cli/pkg/python"
	"github.com/datablast-analytics/blast-cli/pkg/query"
	"github.com/datablast-analytics/blast-cli/pkg/scheduler"
	"github.com/urfave/cli/v2"
)

func Run(isDebug *bool) *cli.Command {
	return &cli.Command{
		Name:      "run",
		Usage:     "run a Blast pipeline",
		ArgsUsage: "[path to the task file]",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "downstream",
				Usage: "pass this flag if you'd like to run all the downstream tasks as well",
			},
			&cli.IntFlag{
				Name:  "workers",
				Usage: "number of workers to run the tasks in parallel",
				Value: 8,
			},
			&cli.StringFlag{
				Name:        "start-date",
				Usage:       "the start date of the range the pipeline will run for in YYYY-MM-DD or YYYY-MM-DD HH:MM:SS format",
				DefaultText: fmt.Sprintf("yesterday, e.g. %s", time.Now().AddDate(0, 0, -1).Format("2006-01-02")),
				Value:       time.Now().AddDate(0, 0, -1).Format("2006-01-02"),
			},
			&cli.StringFlag{
				Name:        "end-date",
				Usage:       "the end date of the range the pipeline will run for in YYYY-MM-DD or YYYY-MM-DD HH:MM:SS format",
				DefaultText: fmt.Sprintf("today, e.g. %s", time.Now().Format("2006-01-02")),
				Value:       time.Now().Format("2006-01-02"),
			},
		},
		Action: func(c *cli.Context) error {
			logger := makeLogger(*isDebug)

			inputPath := c.Args().Get(0)
			if inputPath == "" {
				errorPrinter.Printf("Please give a task or pipeline path: blast-cli run <path to the task definition>)\n")
				return cli.Exit("", 1)
			}

			startDate, err := date.ParseTime(c.String("start-date"))
			logger.Debug("given start date: ", startDate)
			if err != nil {
				errorPrinter.Printf("Please give a valid start date: blast-cli run --start-date <start date>)\n")
				errorPrinter.Printf("A valid start date can be in the YYYY-MM-DD or YYYY-MM-DD HH:MM:SS formats. \n")
				errorPrinter.Printf("    e.g. %s  \n", time.Now().AddDate(0, 0, -1).Format("2006-01-02"))
				errorPrinter.Printf("    e.g. %s  \n", time.Now().AddDate(0, 0, -1).Format("2006-01-02 15:04:05"))
				return cli.Exit("", 1)
			}

			endDate, err := date.ParseTime(c.String("end-date"))
			logger.Debug("given end date: ", endDate)
			if err != nil {
				errorPrinter.Printf("Please give a valid end date: blast-cli run --start-date <start date>)\n")
				errorPrinter.Printf("A valid start date can be in the YYYY-MM-DD or YYYY-MM-DD HH:MM:SS formats. \n")
				errorPrinter.Printf("    e.g. %s  \n", time.Now().AddDate(0, 0, -1).Format("2006-01-02"))
				errorPrinter.Printf("    e.g. %s  \n", time.Now().AddDate(0, 0, -1).Format("2006-01-02 15:04:05"))
				return cli.Exit("", 1)
			}

			pipelinePath := inputPath

			runningForATask := isPathReferencingTask(inputPath)
			var task *pipeline.Task

			runDownstreamTasks := false
			if runningForATask {
				task, err = builder.CreateTaskFromFile(inputPath)
				if err != nil {
					errorPrinter.Printf("Failed to build task: %v\n", err.Error())
					return cli.Exit("", 1)
				}

				if task == nil {
					errorPrinter.Printf("The given file path doesn't seem to be a Blast task definition: '%s'\n", inputPath)
					return cli.Exit("", 1)
				}

				pipelinePath, err = path.GetPipelineRootFromTask(inputPath, pipelineDefinitionFile)
				if err != nil {
					errorPrinter.Printf("Failed to find the pipeline this task belongs to: '%s'\n", inputPath)
					return cli.Exit("", 1)
				}

				if c.Bool("downstream") {
					infoPrinter.Println("The downstream tasks will be executed as well.")
					runDownstreamTasks = true
				}
			}

			if !runningForATask && c.Bool("downstream") {
				infoPrinter.Println("Ignoring the '--downstream' flag since you are running the whole pipeline")
			}

			foundPipeline, err := builder.CreatePipelineFromPath(pipelinePath)
			if err != nil {
				errorPrinter.Println("failed to build pipeline, are you sure you have referred the right path?")
				errorPrinter.Println("\nHint: You need to run this command with a path to either the pipeline directory or the asset file itself directly.")

				return cli.Exit("", 1)
			}

			if !runningForATask {
				rules, err := lint.GetRules(logger, fs)
				if err != nil {
					errorPrinter.Printf("An error occurred while linting the pipelines: %v\n", err)
					return cli.Exit("", 1)
				}

				linter := lint.NewLinter(path.GetPipelinePaths, builder, rules, logger)
				res, err := linter.LintPipelines([]*pipeline.Pipeline{foundPipeline})
				err = reportLintErrors(res, err, lint.Printer{RootCheckPath: pipelinePath})
				if err != nil {
					return cli.Exit("", 1)
				}
			}

			s := scheduler.NewScheduler(logger, foundPipeline)

			infoPrinter.Printf("\nStarting the pipeline execution...\n\n")

			if task != nil {
				logger.Debug("marking single task to run: ", task.Name)
				s.MarkAll(scheduler.Succeeded)
				s.MarkTask(task, scheduler.Pending, runDownstreamTasks)
			}

			executors := executor.DefaultExecutors
			if s.WillRunTaskOfType(executor.TaskTypePython) {
				executors[executor.TaskTypePython] = python.NewLocalOperator(map[string]string{})
			}

			if s.WillRunTaskOfType(executor.TaskTypeBigqueryQuery) {
				wholeFileExtractor := &query.WholeFileExtractor{
					Fs:       fs,
					Renderer: query.NewJinjaRendererFromStartEndDates(&startDate, &endDate),
				}

				bqOperator, err := bigquery.NewBasicOperatorFromGlobals(wholeFileExtractor, bigquery.Materializer{})
				if err != nil {
					errorPrinter.Printf(err.Error())
					return cli.Exit("", 1)
				}

				executors[executor.TaskTypeBigqueryQuery] = bqOperator
			}

			ex := executor.NewConcurrent(logger, executors, c.Int("workers"))
			ex.Start(s.WorkQueue, s.Results)

			start := time.Now()
			results := s.Run(context.Background())
			duration := time.Since(start)

			successPrinter.Printf("\n\nExecuted %d tasks in %s\n", len(results), duration.Truncate(time.Millisecond).String())
			errors := make([]*scheduler.TaskExecutionResult, 0)
			for _, res := range results {
				if res.Error != nil {
					errors = append(errors, res)
				}
			}

			if len(errors) > 0 {
				errorPrinter.Printf("\nFailed tasks: %d\n", len(errors))
				for _, t := range errors {
					errorPrinter.Printf("  - %s\n", t.Instance.Task.Name)
					errorPrinter.Printf("    └── %s\n\n", t.Error.Error())
				}

				upstreamFailedTasks := s.GetTaskInstancesByStatus(scheduler.UpstreamFailed)
				if len(upstreamFailedTasks) > 0 {
					errorPrinter.Printf("The following tasks are skipped due to their upstream failing:\n")
					for _, t := range upstreamFailedTasks {
						errorPrinter.Printf("  - %s\n", t.Task.Name)
					}
				}
			}

			return nil
		},
	}
}

func isPathReferencingTask(p string) bool {
	if strings.HasSuffix(p, pipelineDefinitionFile) {
		return false
	}

	if isDir(p) {
		return false
	}

	return true
}

func isDir(path string) bool {
	fileInfo, err := os.Stat(path)
	if err != nil {
		return false
	}

	return fileInfo.IsDir()
}
