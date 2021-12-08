package path

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const testPipelinePath = "../../testdata/pipelines"

func TestGetPipelinePaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                   string
		root                   string
		pipelineDefinitionFile string
		want                   []string
		wantErr                bool
	}{
		{
			name:                   "pipelines are found",
			root:                   testPipelinePath,
			pipelineDefinitionFile: "pipeline.yml",
			want: []string{
				"../../testdata/pipelines/first-pipeline",
				"../../testdata/pipelines/second-pipeline",
			},
		},
		{
			name:                   "filepath errors are propagated",
			root:                   "some-random-directory-name-that-does-not-exist",
			pipelineDefinitionFile: "pipeline.yml",
			wantErr:                true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := GetPipelinePaths(tt.root, tt.pipelineDefinitionFile)

			if (err != nil) != tt.wantErr {
				t.Errorf("GetPipelinePaths() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			require.Equal(t, tt.want, got)
		})
	}
}
