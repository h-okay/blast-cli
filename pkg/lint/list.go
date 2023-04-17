package lint

import (
	"github.com/datablast-analytics/blast/pkg/bigquery"
	"github.com/datablast-analytics/blast/pkg/executor"
	"github.com/datablast-analytics/blast/pkg/query"
	"github.com/datablast-analytics/blast/pkg/snowflake"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"go.uber.org/zap"
)

func GetRules(logger *zap.SugaredLogger, fs afero.Fs) ([]Rule, error) {
	rules := []Rule{
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
		&SimpleRule{
			Identifier: "valid-task-schedule",
			Validator:  EnsureTaskScheduleIsValid,
		},
		&SimpleRule{
			Identifier: "valid-athena-sql-task",
			Validator:  EnsureAthenaSQLTypeTasksHasDatabaseAndS3FilePath,
		},
		&SimpleRule{
			Identifier: "valid-slack-notification",
			Validator:  EnsureSlackFieldInPipelineIsValid,
		},
		&SimpleRule{
			Identifier: "valid-start-date",
			Validator:  EnsureStartDateIsValid,
		},
	}
	//
	//connectionManager := &connection.Manager{}
	//for _, connection := range conf.E {
	//
	//}

	//rules, err := appendSnowflakeValidatorIfExists(logger, fs, rules)
	//if err != nil {
	//	return nil, err
	//}
	//
	//rules, err = appendBigqueryValidatorIfExists(logger, fs, rules)
	//if err != nil {
	//	return nil, err
	//}

	logger.Debugf("successfully loaded %d rules", len(rules))

	return rules, nil
}

func appendSnowflakeValidatorIfExists(logger *zap.SugaredLogger, fs afero.Fs, rules []Rule) ([]Rule, error) {
	sfConfig, err := snowflake.LoadConfigFromEnv()
	if err != nil {
		return rules, err
	}

	if !sfConfig.IsValid() {
		logger.Debug("no snowflake credentials found in env variables, skipping snowflake validation")
		return rules, nil
	}

	logger.Debug("snowflake config is valid, pinging the database to check if we can connect")
	sf, err := snowflake.NewDB(sfConfig, logger)
	if err != nil {
		return nil, err
	}
	logger.Debug("snowflake ping is successful, adding the validator to the list of rules")

	snowflakeValidator := &QueryValidatorRule{
		Identifier: "snowflake-validator",
		TaskType:   executor.TaskTypeSnowflakeQuery,
		Validator:  sf,
		Extractor: &query.FileQuerySplitterExtractor{
			Fs:       fs,
			Renderer: query.DefaultJinjaRenderer,
		},
		WorkerCount: 32,
		Logger:      logger,
	}

	return append(rules, snowflakeValidator), nil
}

func appendBigqueryValidatorIfExists(logger *zap.SugaredLogger, fs afero.Fs, rules []Rule) ([]Rule, error) {
	config, err := bigquery.LoadConfigFromEnv()
	if err != nil {
		return rules, errors.Wrap(err, "failed to load bigquery config from env")
	}

	if !config.IsValid() {
		logger.Debug("no bigquery credentials found in env variables, skipping bigquery validation")
		return rules, nil
	}

	logger.Debug("bigquery config is valid, appending the rule")
	bq, err := bigquery.NewDB(config)
	if err != nil {
		return nil, err
	}

	bqValidator := &QueryValidatorRule{
		Identifier: "bigquery-validator",
		TaskType:   executor.TaskTypeBigqueryQuery,
		Validator:  bq,
		Extractor: &query.WholeFileExtractor{
			Fs:       fs,
			Renderer: query.DefaultJinjaRenderer,
		},
		WorkerCount: 32,
		Logger:      logger,
	}

	return append(rules, bqValidator), nil
}
