package lint

import (
	"fmt"
	"path/filepath"

	"github.com/datablast-analytics/blast-cli/pkg/pipeline"
	"github.com/fatih/color"
)

type Printer struct{}

type taskSummary struct {
	rule   *Rule
	issues []*Issue
}

var (
	faint           = color.New(color.Faint).SprintFunc()
	successPrinter  = color.New(color.FgGreen)
	pipelinePrinter = color.New(color.FgBlue, color.Bold)
	taskNamePrinter = color.New(color.FgYellow, color.Bold)
	issuePrinter    = color.New(color.FgRed)
	contextPrinter  = color.New(color.FgRed, color.Concealed)
)

func (l *Printer) PrintIssues(analysis *PipelineAnalysisResult) {
	for _, pipelineIssues := range analysis.Pipelines {
		printPipelineSummary(pipelineIssues)
	}
}

func printPipelineSummary(pipelineIssues *PipelineIssues) {
	successPrinter.Println()

	pipelineDirectory := filepath.Dir(pipelineIssues.Pipeline.DefinitionFile.Path)
	pipelinePrinter.Printf("Pipeline: %s %s\n", pipelineIssues.Pipeline.Name, faint(fmt.Sprintf("(%s)", pipelineDirectory)))

	if len(pipelineIssues.Issues) == 0 {
		successPrinter.Println("  No issues found")
		return
	}

	genericIssues := make([]*taskSummary, 0, len(pipelineIssues.Issues))
	taskIssueMap := make(map[*pipeline.Task]*taskSummary)

	for rule, issues := range pipelineIssues.Issues {
		genericIssuesForRule := &taskSummary{
			rule:   rule,
			issues: []*Issue{},
		}

		for _, issue := range issues {
			if issue.Task == nil {
				generisIssues := genericIssuesForRule.issues
				generisIssues = append(generisIssues, issue)
				genericIssuesForRule.issues = generisIssues
				continue
			}

			// create the defaults if there are no issues for this task yet
			if _, ok := taskIssueMap[issue.Task]; !ok {
				taskIssueMap[issue.Task] = &taskSummary{
					rule:   rule,
					issues: []*Issue{},
				}
			}

			taskIssues := taskIssueMap[issue.Task].issues
			taskIssues = append(taskIssues, issue)

			taskIssueMap[issue.Task] = &taskSummary{
				rule:   rule,
				issues: taskIssues,
			}
		}

		if len(genericIssuesForRule.issues) > 0 {
			genericIssues = append(genericIssues, genericIssuesForRule)
		}
	}

	for _, taskSummary := range genericIssues {
		printIssues(taskSummary.rule, taskSummary.issues)
	}

	for task, summary := range taskIssueMap {
		taskNamePrinter.Printf("  %s (%s)\n", task.Name, task.DefinitionFile.Path)
		printIssues(summary.rule, summary.issues)

		issuePrinter.Println()
	}
}

func printIssues(rule *Rule, issues []*Issue) {
	issueCount := len(issues)
	for index, issue := range issues {
		connector := "├──"
		if index == issueCount-1 {
			connector = "└──"
		}

		issuePrinter.Printf("    %s %s %s\n", connector, issue.Description, faint(fmt.Sprintf("(%s)", rule.Name)))
		printIssueContext(issue.Context, index == issueCount-1)
	}
}

func printIssueContext(context []string, lastIssue bool) {
	issueCount := len(context)
	beginning := "│"
	if lastIssue {
		beginning = " "
	}

	for index, row := range context {
		connector := "├─"
		if index == issueCount-1 {
			connector = "└─"
		}

		contextPrinter.Printf("    %s   %s %s\n", beginning, connector, row)
	}
}
