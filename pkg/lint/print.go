package lint

import (
	"path/filepath"

	"github.com/fatih/color"
)

type Printer struct{}

func (l *Printer) PrintIssues(analysis *PipelineAnalysisResult) {
	successPrinter := color.New(color.FgGreen, color.Bold)

	for _, pipelineIssues := range analysis.Pipelines {
		successPrinter.Println()
		issuePrinter := color.New(color.FgRed, color.Bold)

		pipelineDirectory := filepath.Dir(pipelineIssues.Pipeline.DefinitionFile.Path)
		color.Yellow("Pipeline: %s (%s)", pipelineIssues.Pipeline.Name, pipelineDirectory)

		if len(pipelineIssues.Issues) == 0 {
			successPrinter.Println("  No issues found")
			continue
		}

		for rule, issues := range pipelineIssues.Issues {
			for _, issue := range issues {
				issuePrinter.Printf("  %s: %s - %s\n", rule.Name, issue.Task.Name, issue.Description)
			}
		}
	}
}
