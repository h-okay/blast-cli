package lint

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/datablast-analytics/blast-cli/pkg/pipeline"
	"github.com/pkg/errors"
	"github.com/robfig/cron/v3"
	"github.com/spf13/afero"
	"github.com/yourbasic/graph"
)

const (
	validIDRegex = `^[\w.-]+$`

	taskNameMustExist          = `A task must have a name`
	taskNameMustBeAlphanumeric = `A task name must be made of alphanumeric characters, dashes, dots and underscores`

	executableFileCannotBeEmpty   = `The 'run' option cannot be empty, make sure you have defined a file to run`
	executableFileDoesNotExist    = `The executable file does not exist`
	executableFileIsADirectory    = `The executable file is a directory, must be a file`
	executableFileIsEmpty         = `The executable file is empty`
	executableFileIsNotExecutable = "Executable file is not executable, give it the '644' or '755' permissions"

	pipelineNameCannotBeEmpty      = "The pipeline name cannot be empty, it must be a valid name made of alphanumeric characters, dashes, dots and underscores"
	pipelineNameMustBeAlphanumeric = "The pipeline name must be made of alphanumeric characters, dashes, dots and underscores"

	pipelineContainsCycle = "The pipeline has a cycle with dependencies, make sure there are no cyclic dependencies"

	taskScheduleDayDoesNotExist = "Task schedule day must be a valid weekday"

	athenaSQLEmptyDatabaseField   = "Database field cannot be empty"
	athenaSQLInvalidS3FilePath    = "S3 file must start with s3://"
	athenaSQLInvalidDatabaseField = "Database filed must be a valid"
	athenaSQEmptyS3FilePath       = "s3 file path cannot be empty"

	pipelineSlackFieldEmptyName       = "Name in pipeline slack field is cannot be empty"
	pipelineSlackFieldEmptyConnection = "Connection in pipeline slack field is cannot be empty"
	pipelineSlackNameFieldNotUnique   = "Name in pipeline slack field must be a unique value"
)

const (
	taskTypePython         = "python"
	taskTypeSnowflakeQuery = "sf.sql"
	taskTypeBigqueryQuery  = "bq.sql"
)

var validTaskTypes = map[string]struct{}{
	taskTypeBigqueryQuery:                  {},
	"bq.sensor.table":                      {},
	"bq.sensor.query":                      {},
	"bq.cost_tracker":                      {},
	"bash":                                 {},
	"bq.transfer":                          {},
	"bq.sensor.partition":                  {},
	"gcs.from.s3":                          {},
	"gcs.sensor.object_sensor_with_prefix": {},
	"gcs.sensor.object":                    {},
	"empty":                                {},
	"athena.sql":                           {},
	"athena.sensor.query":                  {},
	taskTypePython:                         {},
	"s3.sensor.key_sensor":                 {},
	taskTypeSnowflakeQuery:                 {},
}

var validIDRegexCompiled = regexp.MustCompile(validIDRegex)

func EnsureTaskNameIsValid(pipeline *pipeline.Pipeline) ([]*Issue, error) {
	issues := make([]*Issue, 0)

	for _, task := range pipeline.Tasks {
		if task.Name == "" {
			issues = append(issues, &Issue{
				Task:        task,
				Description: taskNameMustExist,
			})

			continue
		}

		if match := validIDRegexCompiled.MatchString(task.Name); !match {
			issues = append(issues, &Issue{
				Task:        task,
				Description: taskNameMustBeAlphanumeric,
			})
		}
	}

	return issues, nil
}

func EnsureTaskNameIsUnique(p *pipeline.Pipeline) ([]*Issue, error) {
	nameFileMapping := make(map[string][]*pipeline.Task)
	for _, task := range p.Tasks {
		if task.Name == "" {
			continue
		}

		if _, ok := nameFileMapping[task.Name]; !ok {
			nameFileMapping[task.Name] = make([]*pipeline.Task, 0)
		}

		nameFileMapping[task.Name] = append(nameFileMapping[task.Name], task)
	}

	issues := make([]*Issue, 0)
	for name, files := range nameFileMapping {
		if len(files) == 1 {
			continue
		}

		taskPaths := make([]string, 0)
		for _, task := range files {
			taskPaths = append(taskPaths, task.DefinitionFile.Path)
		}

		issues = append(issues, &Issue{
			Task:        files[0],
			Description: fmt.Sprintf("Task name '%s' is not unique, please make sure all the task names are unique", name),
			Context:     taskPaths,
		})
	}

	return issues, nil
}

func EnsureExecutableFileIsValid(fs afero.Fs) PipelineValidator {
	return func(p *pipeline.Pipeline) ([]*Issue, error) {
		issues := make([]*Issue, 0)
		for _, task := range p.Tasks {
			if task.DefinitionFile.Type == pipeline.CommentTask {
				continue
			}

			if task.ExecutableFile.Path == "" {
				if task.Type == taskTypePython {
					issues = append(issues, &Issue{
						Task:        task,
						Description: executableFileCannotBeEmpty,
					})
				}
				continue
			}

			fileInfo, err := fs.Stat(task.ExecutableFile.Path)
			if errors.Is(err, os.ErrNotExist) {
				issues = append(issues, &Issue{
					Task:        task,
					Description: executableFileDoesNotExist,
				})
				continue
			}

			if fileInfo.IsDir() {
				issues = append(issues, &Issue{
					Task:        task,
					Description: executableFileIsADirectory,
				})
				continue
			}

			if fileInfo.Size() == 0 {
				issues = append(issues, &Issue{
					Task:        task,
					Description: executableFileIsEmpty,
				})
			}

			if isFileExecutable(fileInfo.Mode()) {
				issues = append(issues, &Issue{
					Task:        task,
					Description: executableFileIsNotExecutable,
				})
			}
		}

		return issues, nil
	}
}

func EnsurePipelineNameIsValid(pipeline *pipeline.Pipeline) ([]*Issue, error) {
	issues := make([]*Issue, 0)
	if pipeline.Name == "" {
		issues = append(issues, &Issue{
			Description: pipelineNameCannotBeEmpty,
		})

		return issues, nil
	}

	if match := validIDRegexCompiled.MatchString(pipeline.Name); !match {
		issues = append(issues, &Issue{
			Description: pipelineNameMustBeAlphanumeric,
		})
	}

	return issues, nil
}

func isFileExecutable(mode os.FileMode) bool {
	return mode&0o111 != 0
}

func EnsureDependencyExists(p *pipeline.Pipeline) ([]*Issue, error) {
	taskMap := map[string]bool{}
	for _, task := range p.Tasks {
		if task.Name == "" {
			continue
		}

		taskMap[task.Name] = true
	}

	issues := make([]*Issue, 0)
	for _, task := range p.Tasks {
		for _, dep := range task.DependsOn {
			if _, ok := taskMap[dep]; !ok {
				issues = append(issues, &Issue{
					Task:        task,
					Description: fmt.Sprintf("Dependency '%s' does not exist", dep),
				})
			}
		}
	}

	return issues, nil
}

func EnsurePipelineScheduleIsValidCron(p *pipeline.Pipeline) ([]*Issue, error) {
	issues := make([]*Issue, 0)
	if p.Schedule == "" {
		return issues, nil
	}

	_, err := cron.ParseStandard(string(p.Schedule))
	if err != nil {
		issues = append(issues, &Issue{
			Description: fmt.Sprintf("Invalid cron schedule '%s'", p.Schedule),
		})
	}

	return issues, nil
}

func EnsureOnlyAcceptedTaskTypesAreThere(p *pipeline.Pipeline) ([]*Issue, error) {
	issues := make([]*Issue, 0)

	for _, task := range p.Tasks {
		if task.Type == "" {
			continue
		}

		if _, ok := validTaskTypes[task.Type]; !ok {
			issues = append(issues, &Issue{
				Task:        task,
				Description: fmt.Sprintf("Invalid task type '%s'", task.Type),
			})
		}
	}

	return issues, nil
}

// EnsurePipelineHasNoCycles ensures that the pipeline is a DAG, and contains no cycles.
// Since the pipelines are directed graphs, strongly connected components mean cycles, therefore
// they would be considered invalid for our pipelines.
// Strong connectivity wouldn't work for tasks that depend on themselves, therefore there's a specific check for that.
func EnsurePipelineHasNoCycles(p *pipeline.Pipeline) ([]*Issue, error) {
	issues := make([]*Issue, 0)

	for _, task := range p.Tasks {
		for _, dep := range task.DependsOn {
			if task.Name == dep {
				issues = append(issues, &Issue{
					Description: pipelineContainsCycle,
					Context:     []string{fmt.Sprintf("Task `%s` depends on itself", task.Name)},
				})
			}
		}
	}

	taskNameToIndex := make(map[string]int, len(p.Tasks))
	for i, task := range p.Tasks {
		taskNameToIndex[task.Name] = i
	}

	g := graph.New(len(p.Tasks))
	for _, task := range p.Tasks {
		for _, dep := range task.DependsOn {
			g.Add(taskNameToIndex[task.Name], taskNameToIndex[dep])
		}
	}

	cycles := graph.StrongComponents(g)
	for _, cycle := range cycles {
		cycleLength := len(cycle)
		if cycleLength == 1 {
			continue
		}

		tasksInCycle := make(map[string]bool, cycleLength)
		for _, taskIndex := range cycle {
			tasksInCycle[p.Tasks[taskIndex].Name] = true
		}

		context := make([]string, 0, cycleLength)
		for _, taskIndex := range cycle {
			task := p.Tasks[taskIndex]
			for _, dep := range task.DependsOn {
				if _, ok := tasksInCycle[dep]; !ok {
					continue
				}

				context = append(context, fmt.Sprintf("%s ➜ %s", task.Name, dep))
			}
		}

		issues = append(issues, &Issue{
			Description: pipelineContainsCycle,
			Context:     context,
		})
	}

	return issues, nil
}

func EnsureTaskScheduleIsValid(p *pipeline.Pipeline) ([]*Issue, error) {
	issues := make([]*Issue, 0)
	days := []string{"monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"}
	for _, task := range p.Tasks {
		var wrongDays []string
		for _, day := range task.Schedule.Days {
			if !isStringInArray(days, strings.ToLower(day)) {
				wrongDays = append(wrongDays, day)
			}
		}
		if wrongDays != nil {
			contextMessages := make([]string, 0)
			for _, wrongDay := range wrongDays {
				contextMessages = append(contextMessages, fmt.Sprintf("Given day: %s", wrongDay))
			}
			issues = append(issues, &Issue{
				Task:        task,
				Description: taskScheduleDayDoesNotExist,
				Context:     contextMessages,
			})
		}
	}

	return issues, nil
}

func isStringInArray(arr []string, str string) bool {
	for _, a := range arr {
		if str == a {
			return true
		}
	}
	return false
}

func EnsureAthenaSQLTypeTasksHasDatabaseAndS3FilePath(p *pipeline.Pipeline) ([]*Issue, error) {
	issues := make([]*Issue, 0)
	for _, task := range p.Tasks {
		if task.Type == "athena.sql" {
			databaseVar, ok := task.Parameters["database"]
			if !ok {
				issues = append(issues, &Issue{
					Task:        task,
					Description: athenaSQLInvalidDatabaseField,
					Context:     []string{"There is no any database field"},
				})
			}
			if ok && databaseVar == "" {
				issues = append(issues, &Issue{
					Task:        task,
					Description: athenaSQLEmptyDatabaseField,
					Context:     []string{fmt.Sprintf("Given database field is: %s", databaseVar)},
				})
			}
			s3FilePathVar, ok := task.Parameters["s3_file_path"]
			if !ok {
				issues = append(issues, &Issue{
					Task:        task,
					Description: athenaSQEmptyS3FilePath,
					Context:     []string{"There is no any s3 file path field"},
				})
			}
			if ok && !strings.HasPrefix(s3FilePathVar, "s3://") {
				issues = append(issues, &Issue{
					Task:        task,
					Description: athenaSQLInvalidS3FilePath,
					Context:     []string{fmt.Sprintf("Given s3 file path is: %s", s3FilePathVar)},
				})
			}
		}
	}

	return issues, nil
}

func EnsureSlackFieldInPipelineIsValid(p *pipeline.Pipeline) ([]*Issue, error) {
	issues := make([]*Issue, 0)

	slackNames := make([]string, 0)
	for _, slack := range p.Notifications.Slack {
		if slack.Name == "" {
			issues = append(issues, &Issue{
				Description: pipelineSlackFieldEmptyName,
			})
		}

		if slack.Connection == "" {
			issues = append(issues, &Issue{
				Description: pipelineSlackFieldEmptyConnection,
			})
		}
		slackNames = append(slackNames, slack.Name)
	}

	for _, slack := range p.Notifications.Slack {
		if len(slackNames) == 1 {
			continue
		}

		if !isStringInArray(slackNames, slack.Name) {
			continue
		}

		issues = append(issues, &Issue{
			Description: pipelineSlackNameFieldNotUnique,
		})
	}

	return issues, nil
}
