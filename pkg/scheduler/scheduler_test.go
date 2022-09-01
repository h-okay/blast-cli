package scheduler

import (
	"testing"

	"github.com/datablast-analytics/blast-cli/pkg/pipeline"
	"github.com/stretchr/testify/assert"
)

func TestScheduler_getScheduleableTasks(t *testing.T) {
	t.Parallel()

	// In the test cases I'll simulate the execution steps of the following graph:
	// task1 -> task3
	// task2 -> task4
	// task3 -> task5
	// task4 -> task5

	tasks := []*pipeline.Task{
		{
			Name: "task11",
		},
		{
			Name: "task21",
		},
		{
			Name:      "task12",
			DependsOn: []string{"task11"},
		},
		{
			Name:      "task22",
			DependsOn: []string{"task21"},
		},
		{
			Name:      "task3",
			DependsOn: []string{"task12", "task22"},
		},
	}

	tests := []struct {
		name          string
		taskInstances map[string]TaskInstanceStatus
		want          []string
	}{
		{
			name: "beginning the pipeline execution",
			taskInstances: map[string]TaskInstanceStatus{
				"task11": Pending,
				"task12": Pending,
				"task21": Pending,
				"task22": Pending,
				"task3":  Pending,
			},
			want: []string{"task11", "task21"},
		},
		{
			name: "both t1 and t2 are running, should get nothing",
			taskInstances: map[string]TaskInstanceStatus{
				"task11": Running,
				"task12": Pending,
				"task21": Running,
				"task22": Pending,
				"task3":  Pending,
			},
			want: []string{},
		},
		{
			name: "t11 succeeded, should get t12",
			taskInstances: map[string]TaskInstanceStatus{
				"task11": Succeeded,
				"task12": Pending,
				"task21": Running,
				"task22": Pending,
				"task3":  Pending,
			},
			want: []string{"task12"},
		},
		{
			name: "t12 succeeded as well, shouldn't get anything yet",
			taskInstances: map[string]TaskInstanceStatus{
				"task11": Succeeded,
				"task12": Succeeded,
				"task21": Running,
				"task22": Pending,
				"task3":  Pending,
			},
			want: []string{},
		},
		{
			name: "t21 succeeded, should get t22",
			taskInstances: map[string]TaskInstanceStatus{
				"task11": Succeeded,
				"task12": Succeeded,
				"task21": Succeeded,
				"task22": Pending,
				"task3":  Pending,
			},
			want: []string{"task22"},
		},
		{
			name: "t22 succeeded as well, should get the final Task",
			taskInstances: map[string]TaskInstanceStatus{
				"task11": Succeeded,
				"task12": Succeeded,
				"task21": Succeeded,
				"task22": Succeeded,
				"task3":  Pending,
			},
			want: []string{"task3"},
		},
		{
			name: "everything succeeded, should get nothing",
			taskInstances: map[string]TaskInstanceStatus{
				"task11": Succeeded,
				"task12": Succeeded,
				"task21": Succeeded,
				"task22": Succeeded,
				"task3":  Succeeded,
			},
			want: []string{},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			taskInstances := make([]*TaskInstance, 0, len(tasks))
			for _, task := range tasks {
				status, ok := tt.taskInstances[task.Name]
				if !ok {
					t.Fatalf("Given Task doesn't have a status set on the test: %s", task.Name)
				}
				taskInstances = append(taskInstances, &TaskInstance{
					Task:   task,
					status: status,
				})
			}

			p := &Scheduler{
				taskInstances: taskInstances,
			}

			got := p.getScheduleableTasks()
			gotNames := make([]string, 0, len(got))
			for _, t := range got {
				gotNames = append(gotNames, t.Task.Name)
			}

			assert.Equal(t, tt.want, gotNames)
		})
	}
}

func TestScheduler_Run(t *testing.T) {
	t.Parallel()

	// In the test cases I'll simulate the execution steps of the following graph:
	// task1 -> task3
	// task2 -> task4
	// task3 -> task5
	// task4 -> task5

	p := &pipeline.Pipeline{
		Tasks: []*pipeline.Task{
			{
				Name: "task11",
			},
			{
				Name: "task21",
			},
			{
				Name:      "task12",
				DependsOn: []string{"task11"},
			},
			{
				Name:      "task22",
				DependsOn: []string{"task21"},
			},
			{
				Name:      "task3",
				DependsOn: []string{"task12", "task22"},
			},
		},
	}

	scheduler := NewScheduler(p)

	scheduler.Tick(&TaskExecutionResult{
		Instance: &TaskInstance{
			Task: &pipeline.Task{
				Name: "start",
			},
			status: Succeeded,
		},
	})

	// ensure the first two tasks are scheduled
	t11 := <-scheduler.WorkQueue
	assert.Equal(t, "task11", t11.Task.Name)

	t21 := <-scheduler.WorkQueue
	assert.Equal(t, "task21", t21.Task.Name)

	// mark t11 as completed
	scheduler.Tick(&TaskExecutionResult{
		Instance: t11,
	})

	// expect t12 to be scheduled
	t12 := <-scheduler.WorkQueue
	assert.Equal(t, "task12", t12.Task.Name)

	// mark t21 as completed
	scheduler.Tick(&TaskExecutionResult{
		Instance: t21,
	})

	// expect t22 to arrive, given that t21 was completed
	t22 := <-scheduler.WorkQueue
	assert.Equal(t, "task22", t22.Task.Name)

	// mark t12 as completed
	scheduler.Tick(&TaskExecutionResult{
		Instance: t12,
	})

	// mark t22 as completed
	finished := scheduler.Tick(&TaskExecutionResult{
		Instance: t22,
	})
	assert.False(t, finished)

	// now that both t12 and t22 are completed, expect t3 to be dispatched
	t3 := <-scheduler.WorkQueue
	assert.Equal(t, "task3", t3.Task.Name)

	// mark t3 as completed
	finished = scheduler.Tick(&TaskExecutionResult{
		Instance: t3,
	})

	assert.True(t, finished)
}
