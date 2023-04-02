package bigquery

import (
	"fmt"
	"strings"

	"github.com/datablast-analytics/blast/pkg/pipeline"
)

type Materializer struct{}

func (m Materializer) Render(task *pipeline.Asset, query string) (string, error) {
	mat := task.Materialization
	if mat.Type == pipeline.MaterializationTypeNone {
		return query, nil
	}

	if mat.Type == pipeline.MaterializationTypeView {
		return fmt.Sprintf("CREATE OR REPLACE VIEW `%s` AS\n%s", task.Name, query), nil
	}

	if mat.Type == pipeline.MaterializationTypeTable {
		strategy := mat.Strategy
		if strategy == pipeline.MaterializationStrategyNone {
			strategy = pipeline.MaterializationStrategyCreateReplace
		}

		if strategy == pipeline.MaterializationStrategyAppend {
			return fmt.Sprintf("INSERT INTO `%s` %s", task.Name, query), nil
		}

		if strategy == pipeline.MaterializationStrategyCreateReplace {
			return buildCreateReplaceQuery(task, query, mat)
		}

		if strategy == pipeline.MaterializationStrategyDeleteInsert {
			return buildIncrementalQuery(task, query, mat, strategy)
		}
	}

	return "", fmt.Errorf("unsupported materialization type `%s`", mat.Type)
}

func buildIncrementalQuery(task *pipeline.Asset, query string, mat pipeline.Materialization, strategy pipeline.MaterializationStrategy) (string, error) {
	if mat.IncrementalKey == "" {
		return "", fmt.Errorf("materialization strategy %s requires the `incremental_key` field to be set", strategy)
	}

	queries := []string{
		"BEGIN TRANSACTION",
		fmt.Sprintf("CREATE TEMP TABLE __blast_tmp AS %s", query),
		fmt.Sprintf("DELETE FROM `%s` WHERE `%s` in (SELECT DISTINCT `%s` FROM __blast_tmp)", task.Name, mat.IncrementalKey, mat.IncrementalKey),
		fmt.Sprintf("INSERT INTO `%s` SELECT * FROM __blast_tmp", task.Name),
		"COMMIT TRANSACTION",
	}

	return strings.Join(queries, "\n") + ";", nil
}

func buildCreateReplaceQuery(task *pipeline.Asset, query string, mat pipeline.Materialization) (string, error) {
	partitionClause := ""
	if mat.PartitionBy != "" {
		partitionClause = fmt.Sprintf("PARTITION BY `%s`", mat.PartitionBy)
	}

	clusterByClause := ""
	if len(mat.ClusterBy) > 0 {
		clusterByClause = fmt.Sprintf("CLUSTER BY `%s`", strings.Join(mat.ClusterBy, "`, `"))
	}

	return fmt.Sprintf("CREATE OR REPLACE TABLE `%s` %s %s AS\n%s", task.Name, partitionClause, clusterByClause, query), nil
}
