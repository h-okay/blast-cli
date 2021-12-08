package path

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
)

func GetPipelinePaths(root, pipelineDefinitionFile string) ([]string, error) {
	var pipelinePaths []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		if strings.HasSuffix(path, pipelineDefinitionFile) {
			pipelinePaths = append(pipelinePaths, filepath.Dir(path))
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("error walking directory: %w", err)
	}

	return pipelinePaths, err
}
