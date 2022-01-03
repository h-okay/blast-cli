package lint

import (
	"fmt"
	"path/filepath"

	"github.com/datablast-analytics/blast-cli/pkg/pipeline"
	"github.com/fatih/color"
)

type Printer struct{}

func (l *Printer) PrintIssues(analysis *PipelineAnalysisResult) {
	successPrinter := color.New(color.FgGreen)
	pipelinePrinter := color.New(color.FgBlue, color.Bold)
	taskNamePrinter := color.New(color.FgYellow, color.Bold)
	issuePrinter := color.New(color.FgRed)

	faint := color.New(color.Faint).SprintFunc()

	for _, pipelineIssues := range analysis.Pipelines {
		successPrinter.Println()

		pipelineDirectory := filepath.Dir(pipelineIssues.Pipeline.DefinitionFile.Path)
		pipelinePrinter.Printf("Pipeline: %s (%s)\n", pipelineIssues.Pipeline.Name, pipelineDirectory)

		if len(pipelineIssues.Issues) == 0 {
			successPrinter.Println("  No issues found")
			continue
		}

		type taskSummary struct {
			rule   *Rule
			issues []*Issue
		}

		taskIssueMap := make(map[*pipeline.Task]taskSummary)

		for rule, issues := range pipelineIssues.Issues {
			for _, issue := range issues {
				if _, ok := taskIssueMap[issue.Task]; !ok {
					taskIssueMap[issue.Task] = taskSummary{
						rule:   rule,
						issues: []*Issue{},
					}
				}

				taskIssues := taskIssueMap[issue.Task].issues
				taskIssues = append(taskIssues, issue)

				taskIssueMap[issue.Task] = taskSummary{
					rule:   rule,
					issues: taskIssues,
				}
			}
		}

		for task, summary := range taskIssueMap {
			taskNamePrinter.Printf("  %s (%s)\n", task.Name, task.DefinitionFile.Path)
			issueCount := len(summary.issues)
			for index, issue := range summary.issues {
				connector := "├──"
				if index == issueCount-1 {
					connector = "└──"
				}

				issuePrinter.Printf("    %s %s %s\n", connector, issue.Description, faint(fmt.Sprintf("(%s)", summary.rule.Name)))
			}

			issuePrinter.Println()
		}
	}
}
