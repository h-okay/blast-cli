package python

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/datablast-analytics/blast-cli/pkg/git"
	"github.com/datablast-analytics/blast-cli/pkg/pipeline"
	"github.com/pkg/errors"
)

type NoRequirementsFoundError struct{}

func (m *NoRequirementsFoundError) Error() string {
	return "no requirements.txt file found for the given module"
}

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

func (*ModulePathFinder) FindRequirementsTxt(repo *git.Repo, executable *pipeline.ExecutableFile) (string, error) {
	executablePath := filepath.Clean(executable.Path)
	if !strings.HasPrefix(executablePath, repo.Path) {
		return "", errors.New("executable is not in the repository")
	}

	requirementsTxt := findFileUntilParent("requirements.txt", filepath.Dir(executablePath), repo.Path)
	if requirementsTxt == "" {
		return "", &NoRequirementsFoundError{}
	}

	return requirementsTxt, nil
}

func findFileUntilParent(file, startDir, stopDir string) string {
	for {
		potentialPath := filepath.Join(startDir, file)
		if _, err := os.Stat(potentialPath); err == nil {
			return potentialPath
		}

		if startDir == stopDir {
			break
		}

		startDir = filepath.Dir(startDir)
	}

	return ""
}
