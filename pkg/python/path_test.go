package python

import (
	"testing"

	"github.com/datablast-analytics/blast-cli/pkg/git"
	"github.com/datablast-analytics/blast-cli/pkg/pipeline"
	"github.com/stretchr/testify/assert"
)

func TestFindModulePath(t *testing.T) {
	t.Parallel()

	type args struct {
		repo       *git.Repo
		executable *pipeline.ExecutableFile
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "the executable is in a different path",
			args: args{
				repo: &git.Repo{
					Path: "/Users/robin/Projects/my-pipeline",
				},
				executable: &pipeline.ExecutableFile{
					Path: "/Users/robin/Projects/other-project/pipeline1/tasks/my-module/script.py",
				},
			},
			wantErr: true,
		},
		{
			name: "can find the module path",
			args: args{
				repo: &git.Repo{
					Path: "/Users/robin/Projects/my-pipeline",
				},
				executable: &pipeline.ExecutableFile{
					Path: "/Users/robin/Projects/my-pipeline/pipeline1/tasks/my-module/script.py",
				},
			},
			want: "pipeline1.tasks.my-module.script",
		},
		{
			name: "can find the module path even with indirect directory references",
			args: args{
				repo: &git.Repo{
					Path: "/Users/robin/Projects/my-pipeline",
				},
				executable: &pipeline.ExecutableFile{
					Path: "/Users/robin/Projects/my-pipeline/../../Projects/my-pipeline/pipeline1/tasks/my-module/script.py",
				},
			},
			want: "pipeline1.tasks.my-module.script",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			finder := &ModulePathFinder{}
			got, err := finder.FindModulePath(tt.args.repo, tt.args.executable)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.want, got)

			if (err != nil) != tt.wantErr {
				t.Errorf("FindModulePath() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("FindModulePath() got = %v, want %v", got, tt.want)
			}
		})
	}
}
