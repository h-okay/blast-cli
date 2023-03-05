package python

import (
	"context"
	"testing"

	"github.com/datablast-analytics/blast-cli/pkg/git"
	"github.com/datablast-analytics/blast-cli/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockRepoFinder struct {
	mock.Mock
}

func (m *mockRepoFinder) Repo(path string) (*git.Repo, error) {
	args := m.Called(path)
	return args.Get(0).(*git.Repo), args.Error(1)
}

type mockModuleFinder struct {
	mock.Mock
}

func (m *mockModuleFinder) FindModulePath(repo *git.Repo, executable *pipeline.ExecutableFile) (string, error) {
	args := m.Called(repo, executable)
	return args.Get(0).(string), args.Error(1)
}

type mockRunner struct {
	mock.Mock
}

func (m *mockRunner) Run(ctx context.Context, repo *git.Repo, module string) error {
	args := m.Called(ctx, repo, module)
	return args.Error(0)
}

func TestLocalOperator_RunTask(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   func(rf *mockRepoFinder, mf *mockModuleFinder, runner *mockRunner)
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "should return an error if the repo finder fails",
			setup: func(rf *mockRepoFinder, mf *mockModuleFinder, runner *mockRunner) {
				rf.On("Repo", "/path/to/file.py").
					Return(&git.Repo{}, assert.AnError)
			},
			wantErr: assert.Error,
		},
		{
			name: "should return an error if the module path finder fails",
			setup: func(rf *mockRepoFinder, mf *mockModuleFinder, runner *mockRunner) {
				repo := &git.Repo{Path: "/path/to/repo"}
				rf.On("Repo", "/path/to/file.py").
					Return(repo, nil)

				mf.On("FindModulePath", repo, mock.Anything).
					Return("", assert.AnError)
			},
			wantErr: assert.Error,
		},
		{
			name: "should call runner if the module is found as well",
			setup: func(rf *mockRepoFinder, mf *mockModuleFinder, runner *mockRunner) {
				repo := &git.Repo{Path: "/path/to/repo"}
				rf.On("Repo", "/path/to/file.py").
					Return(repo, nil)

				mf.On("FindModulePath", repo, mock.Anything).
					Return("path.to.module", nil)

				runner.On("Run", mock.Anything, repo, "path.to.module").
					Return(assert.AnError)
			},
			wantErr: assert.Error,
		},
		{
			name: "should call runner if the module is found as well",
			setup: func(rf *mockRepoFinder, mf *mockModuleFinder, runner *mockRunner) {
				repo := &git.Repo{Path: "/path/to/repo"}
				rf.On("Repo", "/path/to/file.py").
					Return(repo, nil)

				mf.On("FindModulePath", repo, mock.Anything).
					Return("path.to.module", nil)

				runner.On("Run", mock.Anything, repo, "path.to.module").
					Return(nil)
			},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			repo := &mockRepoFinder{}
			module := &mockModuleFinder{}
			runner := &mockRunner{}
			if tt.setup != nil {
				tt.setup(repo, module, runner)
			}

			o := &LocalOperator{
				repoFinder: repo,
				module:     module,
				runner:     runner,
			}

			task := &pipeline.Task{
				ExecutableFile: pipeline.ExecutableFile{
					Path: "/path/to/file.py",
				},
			}

			tt.wantErr(t, o.RunTask(context.Background(), nil, task))
		})
	}
}
