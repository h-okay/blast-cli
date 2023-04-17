package cmd

import (
	"github.com/datablast-analytics/blast/pkg/pipeline"
	"github.com/fatih/color"
	"github.com/spf13/afero"
)

const (
	pipelineDefinitionFile = "pipeline.yml"
)

var (
	fs = afero.NewCacheOnReadFs(afero.NewOsFs(), afero.NewMemMapFs(), 0)

	infoPrinter    = color.New(color.FgYellow)
	errorPrinter   = color.New(color.FgRed, color.Bold)
	successPrinter = color.New(color.FgGreen, color.Bold)

	builderConfig = pipeline.BuilderConfig{
		PipelineFileName:    pipelineDefinitionFile,
		TasksDirectoryNames: []string{"tasks", "assets"},
		TasksFileSuffixes:   []string{"task.yml", "task.yaml", "asset.yml", "asset.yaml"},
	}

	builder = pipeline.NewBuilder(builderConfig, pipeline.CreateTaskFromYamlDefinition(fs), pipeline.CreateTaskFromFileComments(fs), fs)
)
