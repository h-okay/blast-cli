package path

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type parents struct {
	FirstParent  string `yaml:"parent1"`
	SecondParent string `yaml:"parent2"`
}

type family struct {
	Parents  parents  `yaml:"parents"`
	Siblings []string `yaml:"siblings"`
}

type exampleData struct {
	Name    string   `yaml:"name"`
	Middle  string   `yaml:"middle" validate:"required"`
	Surname string   `yaml:"surname"`
	Age     int      `yaml:"age" validate:"required,gte=28"`
	Height  float64  `yaml:"height"`
	Skills  []string `yaml:"skills"`
	Family  family   `yaml:"family"`
}

func Test_readYamlFileFromPath(t *testing.T) {
	t.Parallel()

	type args struct {
		path string
		out  interface{}
	}

	tests := []struct {
		name           string
		args           args
		expectedOutput *exampleData
		wantErr        bool
	}{
		{
			name: "read valid yaml file from path",
			args: args{
				path: "testdata/yamlreader/successful-validation.yml",
				out:  &exampleData{},
			},
			expectedOutput: &exampleData{
				Name:    "jane",
				Middle:  "james",
				Surname: "doe",
				Age:     30,
				Height:  1.65,
				Skills:  []string{"java", "python", "go"},
				Family: family{
					Parents: parents{
						FirstParent:  "mama",
						SecondParent: "papa",
					},
					Siblings: []string{"sister", "brother"},
				},
			},
		},
		{
			name: "read yaml file from path",
			args: args{
				path: "testdata/yamlreader/no-validation.yml",
				out:  &exampleData{},
			},
			wantErr: true,
		},
		{
			name: "file does not exist",
			args: args{
				path: "testdata/yamlreader/some-file-that-doesnt-exist",
			},
			wantErr: true,
		},
		{
			name: "invalid yaml file",
			args: args{
				path: "testdata/yamlreader/invalid.yml",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := readYaml(tt.args.path, tt.args.out)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expectedOutput, tt.args.out)
			}
		})
	}
}
