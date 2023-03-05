package python

import (
	"path/filepath"
	"strings"

	"github.com/datablast-analytics/blast-cli/pkg/git"
	"github.com/datablast-analytics/blast-cli/pkg/pipeline"
	"github.com/pkg/errors"
)

type ModulePathFinder struct{}

func (*ModulePathFinder) FindModulePath(repo *git.Repo, executable *pipeline.ExecutableFile) (string, error) {
	executablePath := filepath.Clean(executable.Path)
	if !strings.HasPrefix(executablePath, repo.Path) {
		return "", errors.New("executable is not in the repository")
	}

	relativePath, err := filepath.Rel(repo.Path, executablePath)
	if err != nil {
		return "", err
	}

	moduleName := strings.ReplaceAll(relativePath, string(filepath.Separator), ".")
	moduleName = strings.TrimSuffix(moduleName, ".py")
	return moduleName, nil
}
