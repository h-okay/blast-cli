package lint

import (
	"context"
	"fmt"
	"sync"

	"github.com/datablast-analytics/blast-cli/pkg/pipeline"
)

type queryValidator interface {
	IsValid(ctx context.Context, query string) (bool, error)
}

type queryExtractor interface {
	ExtractQueriesFromFile(filepath string) ([]string, error)
}

type QueryValidatorRule struct {
	Identifier  string
	TaskType    string
	Validator   queryValidator
	Extractor   queryExtractor
	WorkerCount int
}

func (q QueryValidatorRule) Name() string {
	return q.Identifier
}

func (q QueryValidatorRule) validateTask(task *pipeline.Task, done chan []*Issue) {
	issues := make([]*Issue, 0)

	queries, err := q.Extractor.ExtractQueriesFromFile(task.ExecutableFile.Path)
	if err != nil {
		issues = append(issues, &Issue{
			Task:        task,
			Description: fmt.Sprintf("Cannot read executable file '%s': %+v", task.ExecutableFile.Path, err),
		})

		done <- issues
		return
	}

	if len(queries) == 0 {
		issues = append(issues, &Issue{
			Task:        task,
			Description: fmt.Sprintf("No queries found in executable file '%s'", task.ExecutableFile.Path),
		})

		done <- issues
		return
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(len(queries))

	for _, query := range queries {
		go func(query string) {
			defer wg.Done()

			valid, err := q.Validator.IsValid(context.Background(), query)
			if err != nil {
				mu.Lock()
				issues = append(issues, &Issue{
					Task:        task,
					Description: fmt.Sprintf("Invalid query found at '%s': %+v", query, err),
				})
				mu.Unlock()
			} else if !valid {
				mu.Lock()
				issues = append(issues, &Issue{
					Task:        task,
					Description: fmt.Sprintf("Query '%s' is invalid", query),
				})
				mu.Unlock()
			}
		}(query)
	}

	wg.Wait()
	done <- issues
}

func (q *QueryValidatorRule) Validate(p *pipeline.Pipeline) ([]*Issue, error) {
	issues := make([]*Issue, 0)

	// skip if there are no workers defined
	if q.WorkerCount == 0 {
		return issues, nil
	}

	taskChannel := make(chan *pipeline.Task)
	results := make(chan []*Issue)

	// start the workers
	for i := 0; i < q.WorkerCount; i++ {
		go func() {
			for task := range taskChannel {
				q.validateTask(task, results)
			}
		}()
	}

	processedTaskCount := 0
	for _, task := range p.Tasks {
		if task.Type != q.TaskType {
			continue
		}

		processedTaskCount++
		taskChannel <- task
	}
	close(taskChannel)

	for i := 0; i < processedTaskCount; i++ {
		foundIssues := <-results
		issues = append(issues, foundIssues...)
	}

	return issues, nil
}
