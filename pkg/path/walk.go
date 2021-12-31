package path

import (
	"io/fs"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
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
				return errors.Wrapf(err, "failed to get absolute path for %s", path)
			}

			pipelinePaths = append(pipelinePaths, filepath.Dir(abs))
		}

		return nil
	})
	if err != nil {
		return nil, errors.Wrapf(err, "error walking directory")
	}

	return pipelinePaths, nil
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
			return errors.Wrapf(err, "failed to get absolute path for %s", path)
		}

		paths = append(paths, abs)

		return nil
	})
	if err != nil {
		return nil, errors.Wrapf(err, "error walking directory")
	}

	return paths, nil
}
