package main

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/datablast-analytics/blast-cli/pkg/bigquery"
	"github.com/datablast-analytics/blast-cli/pkg/executor"
	"github.com/datablast-analytics/blast-cli/pkg/lint"
	"github.com/datablast-analytics/blast-cli/pkg/path"
	"github.com/datablast-analytics/blast-cli/pkg/pipeline"
	"github.com/datablast-analytics/blast-cli/pkg/query"
	"github.com/datablast-analytics/blast-cli/pkg/scheduler"
	"github.com/fatih/color"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	defaultPipelinePath    = "."
	pipelineDefinitionFile = "pipeline.yml"
	defaultTasksPath       = "tasks"
)

var (
	fs = afero.NewCacheOnReadFs(afero.NewOsFs(), afero.NewMemMapFs(), 100*time.Second)

	infoPrinter    = color.New(color.FgYellow)
	errorPrinter   = color.New(color.FgRed, color.Bold)
	successPrinter = color.New(color.FgGreen, color.Bold)
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
					logger := makeLogger(isDebug)

					builderConfig := pipeline.BuilderConfig{
						PipelineFileName:   pipelineDefinitionFile,
						TasksDirectoryName: defaultTasksPath,
						TasksFileSuffixes:  []string{"task.yml", "task.yaml"},
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
			{
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
						TasksFileSuffixes:  []string{"task.yml", "task.yaml"},
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
			},
			{
				Name:      "run",
				Usage:     "run a Blast pipeline",
				ArgsUsage: "[path to the task file]",
				Action: func(c *cli.Context) error {
					logger := makeLogger(isDebug)

					pipelinePath := c.Args().Get(0)
					if pipelinePath == "" {
						errorPrinter.Printf("Please give a task or pipeline path: blast-cli run <path to the task definition>)\n")
						return cli.Exit("", 1)
					}

					builderConfig := pipeline.BuilderConfig{
						PipelineFileName:   pipelineDefinitionFile,
						TasksDirectoryName: defaultTasksPath,
						TasksFileName:      defaultTaskFileName,
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
			},
		},
	}

	_ = app.Run(os.Args)
}

func makeLogger(isDebug bool) *zap.SugaredLogger {
	config := zap.Config{
		Level:       zap.NewAtomicLevelAt(zap.InfoLevel),
		Development: false,
		Sampling:    nil,
		Encoding:    "console",
		EncoderConfig: zapcore.EncoderConfig{
			TimeKey:        "ts",
			LevelKey:       "level",
			NameKey:        "logger",
			FunctionKey:    zapcore.OmitKey,
			MessageKey:     "msg",
			StacktraceKey:  "stacktrace",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    zapcore.CapitalLevelEncoder,
			EncodeTime:     zapcore.ISO8601TimeEncoder,
			EncodeDuration: zapcore.StringDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		},
		OutputPaths:      []string{"stderr"},
		ErrorOutputPaths: []string{"stderr"},
	}

	if isDebug {
		config.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
		config.Development = true
		config.EncoderConfig.CallerKey = "caller"
	}

	logger, err := config.Build()
	if err != nil {
		panic(err)
	}

	return logger.Sugar()
}
