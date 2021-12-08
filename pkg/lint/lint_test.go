package lint

import (
	"errors"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestLinter_Lint(t *testing.T) {
	t.Parallel()

	errorRule := func(pipelinePath string) error { return errors.New("first rule failed") }
	successRule := func(pipelinePath string) error { return nil }

	type fields struct {
		pipelineFinder func(rootPath string, pipelineDefinitionFileName string) ([]string, error)
		rules          []Rule
	}

	type args struct {
		rootPath                   string
		pipelineDefinitionFileName string
	}
	tests := []struct {
		name          string
		pipelinePaths []string
		fields        fields
		args          args
		wantErr       bool
	}{
		{
			name: "pipeline finder returned an error",
			fields: fields{
				pipelineFinder: func(root, fileName string) ([]string, error) {
					require.Equal(t, "some-root-path", root)
					require.Equal(t, "some-file-name", fileName)
					return nil, errors.New("cannot find pipelines")
				},
			},
			args: args{
				rootPath:                   "some-root-path",
				pipelineDefinitionFileName: "some-file-name",
			},
			wantErr: true,
		},
		{
			name: "pipeline finder returned a file not found error",
			fields: fields{
				pipelineFinder: func(root, fileName string) ([]string, error) {
					require.Equal(t, "some-root-path", root)
					require.Equal(t, "some-file-name", fileName)
					return nil, os.ErrNotExist
				},
			},
			args: args{
				rootPath:                   "some-root-path",
				pipelineDefinitionFileName: "some-file-name",
			},
			wantErr: true,
		},
		{
			name: "empty file list returned",
			fields: fields{
				pipelineFinder: func(root, fileName string) ([]string, error) {
					require.Equal(t, "some-root-path", root)
					require.Equal(t, "some-file-name", fileName)
					return []string{}, nil
				},
			},
			args: args{
				rootPath:                   "some-root-path",
				pipelineDefinitionFileName: "some-file-name",
			},
			wantErr: true,
		},
		{
			name: "found nested pipelines",
			fields: fields{
				pipelineFinder: func(root, fileName string) ([]string, error) {
					require.Equal(t, "some-root-path", root)
					require.Equal(t, "some-file-name", fileName)
					return []string{"path/to/pipeline1", "path/to/pipeline1/some-other-pipeline/under-here", "path/to/pipeline2"}, nil
				},
			},
			args: args{
				rootPath:                   "some-root-path",
				pipelineDefinitionFileName: "some-file-name",
			},
			wantErr: true,
		},
		{
			name: "rules are properly applied, first rule fails",
			fields: fields{
				pipelineFinder: func(root, fileName string) ([]string, error) {
					require.Equal(t, "some-root-path", root)
					require.Equal(t, "some-file-name", fileName)
					return []string{"path/to/pipeline1", "path/to/pipeline2"}, nil
				},
				rules: []Rule{errorRule, successRule},
			},
			args: args{
				rootPath:                   "some-root-path",
				pipelineDefinitionFileName: "some-file-name",
			},
			wantErr: true,
		},
		{
			name: "rules are properly applied, second rule fails",
			fields: fields{
				pipelineFinder: func(root, fileName string) ([]string, error) {
					require.Equal(t, "some-root-path", root)
					require.Equal(t, "some-file-name", fileName)
					return []string{"path/to/pipeline1", "path/to/pipeline2"}, nil
				},
				rules: []Rule{successRule, errorRule},
			},
			args: args{
				rootPath:                   "some-root-path",
				pipelineDefinitionFileName: "some-file-name",
			},
			wantErr: true,
		},
		{
			name: "rules are properly applied, second rule fails",
			fields: fields{
				pipelineFinder: func(root, fileName string) ([]string, error) {
					require.Equal(t, "some-root-path", root)
					require.Equal(t, "some-file-name", fileName)
					return []string{"path/to/pipeline1", "path/to/pipeline2"}, nil
				},
				rules: []Rule{successRule, successRule},
			},
			args: args{
				rootPath:                   "some-root-path",
				pipelineDefinitionFileName: "some-file-name",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			l := &Linter{findPipelines: tt.fields.pipelineFinder, rules: tt.fields.rules}

			err := l.Lint(tt.args.rootPath, tt.args.pipelineDefinitionFileName)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

		})
	}
}
