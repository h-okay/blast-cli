package lint

import (
	"time"

	"github.com/spf13/afero"
)

func GetRules() []Rule {
	return []Rule{
		&SimpleRule{
			Identifier: "task-name-valid",
			Validator:  EnsureTaskNameIsValid,
		},
		&SimpleRule{
			Identifier: "task-name-valid",
			Validator:  EnsureTaskNameIsValid,
		},
		&SimpleRule{
			Identifier: "task-name-unique",
			Validator:  EnsureTaskNameIsUnique,
		},
		&SimpleRule{
			Identifier: "dependency-exists",
			Validator:  EnsureDependencyExists,
		},
		&SimpleRule{
			Identifier: "valid-executable-file",
			Validator:  EnsureExecutableFileIsValid(afero.NewCacheOnReadFs(afero.NewOsFs(), afero.NewMemMapFs(), 100*time.Second)),
		},
		&SimpleRule{
			Identifier: "valid-pipeline-schedule",
			Validator:  EnsurePipelineScheduleIsValidCron,
		},
		&SimpleRule{
			Identifier: "valid-pipeline-name",
			Validator:  EnsurePipelineNameIsValid,
		},
		&SimpleRule{
			Identifier: "valid-task-type",
			Validator:  EnsureOnlyAcceptedTaskTypesAreThere,
		},
		&SimpleRule{
			Identifier: "acyclic-pipeline",
			Validator:  EnsurePipelineHasNoCycles,
		},
	}
}
