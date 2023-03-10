package python

import (
	"context"
	"testing"

	"github.com/datablast-analytics/blast-cli/pkg/git"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockReqInstaller struct {
	mock.Mock
}

func (m *mockReqInstaller) EnsureVirtualEnvExists(ctx context.Context, repo *git.Repo, requirementsTxt string) (string, error) {
	called := m.Called(ctx, repo, requirementsTxt)
	return called.String(0), called.Error(1)
}

func Test_localPythonRunner_Run(t *testing.T) {
	t.Parallel()

	type fields struct {
		cmd                   cmd
		requirementsInstaller requirementsInstaller
	}

	repo := &git.Repo{}
	// execCtx := &executionContext{}

	module := "path.to.module"
	requirementsTxt := "/path/to/requirements.txt"
	defaultExecContext := &executionContext{
		repo:            repo,
		module:          module,
		requirementsTxt: requirementsTxt,
	}

	venvPath := "/path/to/venv"
	tests := []struct {
		name    string
		fields  func() *fields
		execCtx *executionContext
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "if no dependencies are found the basic command should be executed, and error should be propagated",
			fields: func() *fields {
				cmd := new(mockCmd)
				cmd.On("Run", mock.Anything, repo, &command{
					Name: "python3",
					Args: []string{"-u", "-m", module},
				}).Return(assert.AnError)

				return &fields{
					cmd: cmd,
				}
			},
			execCtx: &executionContext{
				repo:            repo,
				module:          module,
				requirementsTxt: "",
			},
			wantErr: assert.Error,
		},
		{
			name: "if no dependencies are found the basic command should be executed",
			fields: func() *fields {
				cmd := new(mockCmd)
				cmd.On("Run", mock.Anything, repo, &command{
					Name: "python3",
					Args: []string{"-u", "-m", module},
				}).Return(nil)

				return &fields{
					cmd: cmd,
				}
			},
			execCtx: &executionContext{
				repo:            repo,
				module:          module,
				requirementsTxt: "",
			},
			wantErr: assert.NoError,
		},
		{
			name: "if req installation fails then the error must be propagated",
			fields: func() *fields {
				reqs := new(mockReqInstaller)
				reqs.On("EnsureVirtualEnvExists", mock.Anything, repo, requirementsTxt).
					Return("", assert.AnError)

				return &fields{
					cmd:                   new(mockCmd),
					requirementsInstaller: reqs,
				}
			},
			execCtx: defaultExecContext,
			wantErr: assert.Error,
		},
		{
			name: "if there is no requirements path that needs to be sourced then no dependency should be installed",
			fields: func() *fields {
				reqs := new(mockReqInstaller)
				reqs.On("EnsureVirtualEnvExists", mock.Anything, repo, requirementsTxt).
					Return("", nil)

				cmd := new(mockCmd)
				cmd.On("Run", mock.Anything, repo, &command{
					Name: "python3",
					Args: []string{"-u", "-m", module},
				}).Return(nil)

				return &fields{
					cmd:                   cmd,
					requirementsInstaller: reqs,
				}
			},
			execCtx: defaultExecContext,
			wantErr: assert.NoError,
		},
		{
			name: "if venv path is found then it should be sourced, error is propagated",
			fields: func() *fields {
				reqs := new(mockReqInstaller)
				reqs.On("EnsureVirtualEnvExists", mock.Anything, repo, requirementsTxt).
					Return(venvPath, nil)

				expectedCommand := "source /path/to/venv/bin/activate && echo 'activated virtualenv' && pip3 install -r /path/to/requirements.txt --quiet --quiet && echo 'installed all the dependencies' && python3 -u -m path.to.module"

				cmd := new(mockCmd)
				cmd.On("Run", mock.Anything, repo, &command{
					Name: "/bin/sh",
					Args: []string{"-c", expectedCommand},
				}).Return(assert.AnError)

				return &fields{
					cmd:                   cmd,
					requirementsInstaller: reqs,
				}
			},
			execCtx: defaultExecContext,
			wantErr: assert.Error,
		},
		{
			name: "if venv path is found then it should be sourced, no error",
			fields: func() *fields {
				reqs := new(mockReqInstaller)
				reqs.On("EnsureVirtualEnvExists", mock.Anything, repo, requirementsTxt).
					Return(venvPath, nil)

				expectedCommand := "source /path/to/venv/bin/activate && echo 'activated virtualenv' && pip3 install -r /path/to/requirements.txt --quiet --quiet && echo 'installed all the dependencies' && python3 -u -m path.to.module"

				cmd := new(mockCmd)
				cmd.On("Run", mock.Anything, repo, &command{
					Name: "/bin/sh",
					Args: []string{"-c", expectedCommand},
				}).Return(nil)

				return &fields{
					cmd:                   cmd,
					requirementsInstaller: reqs,
				}
			},
			execCtx: defaultExecContext,
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := tt.fields()
			l := &localPythonRunner{
				cmd:                   f.cmd,
				requirementsInstaller: f.requirementsInstaller,
			}
			tt.wantErr(t, l.Run(context.Background(), tt.execCtx))
		})
	}
}
