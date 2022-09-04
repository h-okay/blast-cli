package cmd

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/datablast-analytics/blast-cli/pkg/bigquery"
	"github.com/datablast-analytics/blast-cli/pkg/executor"
	"github.com/datablast-analytics/blast-cli/pkg/path"
	"github.com/datablast-analytics/blast-cli/pkg/pipeline"
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
		},
		Action: func(c *cli.Context) error {
			logger := makeLogger(*isDebug)

			inputPath := c.Args().Get(0)
			if inputPath == "" {
				errorPrinter.Printf("Please give a task or pipeline path: blast-cli run <path to the task definition>)\n")
				return cli.Exit("", 1)
			}

			pipelinePath := inputPath

			runningForATask := isPathReferencingTask(inputPath)
			var task *pipeline.Task
			var err error

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
				errorPrinter.Printf("Failed to build pipeline: %v\n", err.Error())
				return cli.Exit("", 1)
			}

			wholeFileExtractor := &query.WholeFileExtractor{
				Fs:       fs,
				Renderer: query.DefaultRenderer,
			}

			executors := executor.DefaultExecutors

			var bqOperator *bigquery.BasicOperator
			if foundPipeline.HasTaskType("bq.sql") {
				bqOperator, err = bigquery.NewBasicOperatorFromGlobals(wholeFileExtractor)
				if err != nil {
					errorPrinter.Printf(err.Error())
					return cli.Exit("", 1)
				}

				executors[executor.TaskTypeBigqueryQuery] = bqOperator
			}

			s := scheduler.NewScheduler(logger, foundPipeline)
			ex := executor.NewConcurrent(logger, executors, 8)

			ex.Start(s.WorkQueue, s.Results)

			infoPrinter.Printf("\nStarting the pipeline execution...\n\n")

			if task != nil {
				logger.Debug("marking single task to run: ", task.Name)
				s.MarkAll(scheduler.Succeeded)
				s.MarkTask(task, scheduler.Pending, runDownstreamTasks)
			}

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

func RunTask() *cli.Command {
	return &cli.Command{
		Name:      "run-task",
		Usage:     "run a single Blast task",
		ArgsUsage: "[path to the task file]",
		Action: func(c *cli.Context) error {
			taskPath := c.Args().Get(0)
			if taskPath == "" {
				errorPrinter.Printf("Please give a task path: blast-cli run-task <path to the task definition>)\n")
				return cli.Exit("", 1)
			}

			task, err := builder.CreateTaskFromFile(taskPath)
			if err != nil {
				errorPrinter.Printf("Failed to build task: %v\n", err.Error())
				return cli.Exit("", 1)
			}

			if task == nil {
				errorPrinter.Printf("The given file path doesn't seem to be a Blast task definition: '%s'\n", taskPath)
				return cli.Exit("", 1)
			}

			pathToPipeline, err := path.GetPipelineRootFromTask(taskPath, pipelineDefinitionFile)
			if err != nil {
				errorPrinter.Printf("Failed to find the pipeline this task belongs to: '%s'\n", taskPath)
				return cli.Exit("", 1)
			}

			foundPipeline, err := builder.CreatePipelineFromPath(pathToPipeline)
			if err != nil {
				errorPrinter.Printf("Failed to build pipeline: %v\n", err.Error())
				return cli.Exit("", 1)
			}

			wholeFileExtractor := &query.WholeFileExtractor{
				Fs:       fs,
				Renderer: query.DefaultRenderer,
			}

			bqOperator, err := bigquery.NewBasicOperatorFromGlobals(wholeFileExtractor)
			if err != nil {
				errorPrinter.Printf(err.Error())
				return cli.Exit("", 1)
			}

			e := executor.Sequential{
				TaskTypeMap: map[string]executor.Operator{
					"bq.sql": bqOperator,
				},
			}

			err = e.RunSingleTask(context.Background(), foundPipeline, task)
			if err != nil {
				errorPrinter.Printf("Failed to run task: %v\n", err.Error())
				return cli.Exit("", 1)
			}

			successPrinter.Printf("Task '%s' successfully executed.\n", task.Name)

			return nil
		},
	}
}
