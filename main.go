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
	"go.uber.org/zap/zapcore"
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
			{
				Name:      "run-task",
				Usage:     "run a single Blast task",
				ArgsUsage: "[path to the task file]",
				Action: func(c *cli.Context) error {
					errorPrinter := color.New(color.FgRed, color.Bold)

					taskPath := c.Args().Get(0)
					if taskPath == "" {
						errorPrinter.Printf("Please give a task path: blast-cli run-task <path to the task definition>)\n")
						return cli.Exit("", 1)
					}

					builderConfig := pipeline.BuilderConfig{
						PipelineFileName:   pipelineDefinitionFile,
						TasksDirectoryName: defaultTasksPath,
						TasksFileName:      defaultTaskFileName,
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

					_, err = path.GetPipelinePathFromTask(taskPath, pipelineDefinitionFile)
					if err != nil {
						errorPrinter.Printf("Failed to find the pipeline this task belongs to: '%s'\n", taskPath)
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
