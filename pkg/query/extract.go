package query

import (
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

type FileExtractor struct {
	Fs afero.Fs
}

func (f FileExtractor) ExtractQueriesFromFile(filepath string) ([]string, error) {
	queries := make([]string, 0)
	contents, err := afero.ReadFile(f.Fs, filepath)
	if err != nil {
		return queries, errors.Wrap(err, "could not read file")
	}

	return splitQueries(string(contents)), nil
}

func splitQueries(fileContent string) []string {
	queries := make([]string, 0)
	for _, query := range strings.Split(fileContent, ";") {
		query = strings.TrimSpace(query)
		if len(query) == 0 {
			continue
		}

		queryLines := strings.Split(query, "\n")
		cleanQueryRows := make([]string, 0, len(queryLines))
		for _, line := range queryLines {
			line = strings.TrimSpace(line)
			if len(line) == 0 {
				continue
			}

			if strings.HasPrefix(line, "--") {
				continue
			}

			cleanQueryRows = append(cleanQueryRows, line)
		}

		cleanQuery := strings.Join(cleanQueryRows, "\n")
		queries = append(queries, cleanQuery)
	}

	return queries
}
