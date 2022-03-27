package lint

import (
	"time"

	"github.com/datablast-analytics/blast-cli/pkg/query"
	"github.com/datablast-analytics/blast-cli/pkg/snowflake"
	"github.com/spf13/afero"
	"go.uber.org/zap"
)

func GetRules(logger *zap.SugaredLogger) ([]Rule, error) {
	fs := afero.NewCacheOnReadFs(afero.NewOsFs(), afero.NewMemMapFs(), 100*time.Second)
	rules := []Rule{
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
			Validator:  EnsureExecutableFileIsValid(fs),
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

	sfConfig, err := snowflake.LoadConfigFromEnv()
	if err != nil {
		return nil, err
	}

	if sfConfig.IsValid() {
		logger.Debug("snowflake config is valid, pinging the database to check if we can connect")
		sf, err := snowflake.NewDB(sfConfig, logger)
		if err != nil {
			return nil, err
		}
		logger.Debug("snowflake ping is successful, adding the validator to the list of rules")

		renderer := &query.Renderer{
			Args: map[string]string{
				"ds":                   time.Now().Format("2006-01-02"),
				"ds_nodash":            time.Now().Format("20060102"),
				"macros.ds_add(ds, 1)": time.Now().Add(24 * time.Hour).Format("2006-01-02"),
			},
		}
		queryExtractor := query.FileExtractor{
			Fs:       fs,
			Renderer: renderer,
		}

		snowflakeValidator := &QueryValidatorRule{
			Identifier:  "snowflake-validator",
			TaskType:    taskTypeSnowflakeQuery,
			Validator:   sf,
			Extractor:   &queryExtractor,
			WorkerCount: 32,
			Logger:      logger,
		}

		rules = append(rules, snowflakeValidator)
	} else {
		logger.Debug("no snowflake credentials found in env variables, skipping snowflake validation")
	}

	logger.Debugf("starting validation for %d rules", len(rules))

	return rules, nil
}
