package path

import (
	"io/ioutil"
	"path/filepath"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

func ReadYaml(path string, out interface{}) error {
	buf, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}

	err = yaml.Unmarshal(buf, out)
	if err != nil {
		return errors.Wrapf(err, "cannot read the pipeline definition at '%s'", path)
	}

	validate := validator.New()
	err = validate.Struct(out)
	if err != nil {
		return errors.Wrapf(err, "cannot validate the YAML file at '%s'", path)
	}

	return nil
}

// ExcludeSubItemsInDirectoryContainingFile cleans up the list to remove sub-paths that are in the same directory as
// the file. The primary usage of this is to remove the sub-paths for the directory that contains `task.yml`.
func ExcludeSubItemsInDirectoryContainingFile(filePaths []string, file string) []string {
	result := make([]string, 0, len(filePaths))

	var targetsToRemove []string
	for _, path := range filePaths {
		if strings.HasSuffix(path, file) {
			targetsToRemove = append(targetsToRemove, filepath.Dir(path))
		}
	}

	for _, path := range filePaths {
		shouldBeIncluded := true
		for _, target := range targetsToRemove {
			if strings.HasPrefix(path, target) && path != filepath.Join(target, file) {
				shouldBeIncluded = false
				break
			}
		}

		if shouldBeIncluded {
			result = append(result, path)
		}
	}

	return result
}
