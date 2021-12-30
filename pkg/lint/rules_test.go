package lint

import (
	"github.com/datablast-analytics/blast-cli/pkg/pipeline"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestEnsureNameExists(t *testing.T) {
	t.Parallel()

	taskWithEmptyName := pipeline.Task{
		Name: "",
	}

	type args struct {
		pipeline *pipeline.Pipeline
	}
	tests := []struct {
		name    string
		args    args
		want    []*Issue
		wantErr bool
	}{
		{
			name: "all tasks have names, no error",
			args: args{
				pipeline: &pipeline.Pipeline{
					Name: "test",
					Tasks: []*pipeline.Task{
						{
							Name: "task1",
						},
						{
							Name: "task2",
						},
					},
				},
			},
			want:    make([]*Issue, 0),
			wantErr: false,
		},
		{
			name: "tasks with missing name are reported",
			args: args{
				pipeline: &pipeline.Pipeline{
					Name: "test",
					Tasks: []*pipeline.Task{
						{
							Name: "task1",
						},
						&taskWithEmptyName,
						{
							Name: "some-other-task",
						},
					},
				},
			},
			want:    []*Issue{
				{
					Task:        &taskWithEmptyName,
					Description: nameExistsDescription,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := EnsureNameExists(tt.args.pipeline)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tt.want, got)
		})
	}
}
