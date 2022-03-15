package query

import (
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func TestFileExtractor_ExtractQueriesFromFile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		setupFilesystem func(t *testing.T, fs afero.Fs)
		path            string
		want            []string
		wantErr         bool
	}{
		{
			name:    "file doesnt exist, fail",
			path:    "somefile.txt",
			want:    make([]string, 0),
			wantErr: true,
		},
		{
			name: "single query",
			path: "somefile.txt",
			setupFilesystem: func(t *testing.T, fs afero.Fs) {
				err := afero.WriteFile(fs, "somefile.txt", []byte("select * from users;"), 0o644)
				require.NoError(t, err)
			},
			want: []string{"select * from users"},
		},
		{
			name: "multiple queries, multiline",
			path: "somefile.txt",
			setupFilesystem: func(t *testing.T, fs afero.Fs) {
				query := `select * from users;
;;
							select name from countries;;
							`
				err := afero.WriteFile(fs, "somefile.txt", []byte(query), 0o644)
				require.NoError(t, err)
			},
			want: []string{"select * from users", "select name from countries"},
		},
		{
			name: "multiple queries, multiline, starts with a comment",
			path: "somefile.txt",
			setupFilesystem: func(t *testing.T, fs afero.Fs) {
				query := `
-- here's some comment
select * from users;
;;
							select name from countries;;
							`
				err := afero.WriteFile(fs, "somefile.txt", []byte(query), 0o644)
				require.NoError(t, err)
			},
			want: []string{"select * from users", "select name from countries"},
		},
		{
			name: "multiple queries, multiline, comments in the middle",
			path: "somefile.txt",
			setupFilesystem: func(t *testing.T, fs afero.Fs) {
				query := `
-- here's some comment
select * from users;
;;
-- here's some other comment
	-- and a nested one event
select name from countries;;
							`
				err := afero.WriteFile(fs, "somefile.txt", []byte(query), 0o644)
				require.NoError(t, err)
			},
			want: []string{"select * from users", "select name from countries"},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fs := afero.NewMemMapFs()
			if tt.setupFilesystem != nil {
				tt.setupFilesystem(t, fs)
			}

			f := FileExtractor{
				Fs: fs,
			}

			got, err := f.ExtractQueriesFromFile(tt.path)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tt.want, got)
		})
	}
}
