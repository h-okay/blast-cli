package cmd

import (
	"context"
	"sync"

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
		Action: func(c *cli.Context) error {
			logger := makeLogger(*isDebug)

			pipelinePath := c.Args().Get(0)
			if pipelinePath == "" {
				errorPrinter.Printf("Please give a task or pipeline path: blast-cli run <path to the task definition>)\n")
				return cli.Exit("", 1)
			}

			builderConfig := pipeline.BuilderConfig{
				PipelineFileName:   pipelineDefinitionFile,
				TasksDirectoryName: defaultTasksPath,
				TasksFileSuffixes:  defaultTaskFileSuffixes,
			}
			builder := pipeline.NewBuilder(builderConfig, pipeline.CreateTaskFromYamlDefinition, pipeline.CreateTaskFromFileComments)

			foundPipeline, err := builder.CreatePipelineFromPath(pipelinePath)
			if err != nil {
				errorPrinter.Printf("Failed to build pipeline: %v\n", err.Error())
				return cli.Exit("", 1)
			}

			wholeFileExtractor := &query.WholeFileExtractor{
				Fs:       fs,
				Renderer: query.DefaultRenderer,
			}

			var bqOperator *bigquery.BasicOperator
			if foundPipeline.HasTaskType("bq.sql") {
				bqOperator, err = bigquery.NewBasicOperatorFromGlobals(wholeFileExtractor)
				if err != nil {
					errorPrinter.Printf(err.Error())
					return cli.Exit("", 1)
				}
			}

			s := scheduler.NewScheduler(logger, foundPipeline)
			ex := executor.NewConcurrent(logger, map[string]executor.Operator{
				"empty":  executor.EmptyOperator{},
				"bq.sql": bqOperator,
			}, 8)
			ex.Start(s.WorkQueue, s.Results)

			infoPrinter.Println("Starting the pipeline execution...")

			var wg sync.WaitGroup
			wg.Add(1)
			s.Run(context.Background(), &wg)
			wg.Wait()

			successPrinter.Println("Pipeline has been completed successfully")

			return nil
		},
	}
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

			builderConfig := pipeline.BuilderConfig{
				PipelineFileName:   pipelineDefinitionFile,
				TasksDirectoryName: defaultTasksPath,
				TasksFileSuffixes:  defaultTaskFileSuffixes,
			}
			builder := pipeline.NewBuilder(builderConfig, pipeline.CreateTaskFromYamlDefinition, pipeline.CreateTaskFromFileComments)

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
