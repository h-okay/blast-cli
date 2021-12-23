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
			abs, err := filepath.Abs(path)
			if err != nil {
				return fmt.Errorf("failed to get absolute path for %s: %s", path, err)
			}

			pipelinePaths = append(pipelinePaths, filepath.Dir(abs))
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("error walking directory: %w", err)
	}

	return pipelinePaths, err
}

func GetAllFilesRecursive(root string) ([]string, error) {
	var paths []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		abs, err := filepath.Abs(path)
		if err != nil {
			return fmt.Errorf("failed to get absolute path for %s: %s", path, err)
		}

		paths = append(paths, abs)

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("error walking directory: %w", err)
	}

	return paths, err
}
