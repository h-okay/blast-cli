package bigquery

import (
	"testing"

	"github.com/datablast-analytics/blast-cli/pkg/pipeline"
	"github.com/stretchr/testify/assert"
)

func TestMaterializer_Render(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		task    *pipeline.Task
		query   string
		want    string
		wantErr bool
	}{
		{
			name:  "no materialization, return raw query",
			task:  &pipeline.Task{},
			query: "SELECT 1",
			want:  "SELECT 1",
		},
		{
			name: "materialize to a view",
			task: &pipeline.Task{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type: pipeline.MaterializationTypeView,
				},
			},
			query: "SELECT 1",
			want:  "CREATE OR REPLACE VIEW `my.asset` AS\nSELECT 1",
		},
		{
			name: "materialize to a table, no partition or cluster, default to create+replace",
			task: &pipeline.Task{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type: pipeline.MaterializationTypeTable,
				},
			},
			query: "SELECT 1",
			want:  "CREATE OR REPLACE TABLE `my.asset`   AS\nSELECT 1",
		},
		{
			name: "materialize to a table with partition, no cluster",
			task: &pipeline.Task{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:        pipeline.MaterializationTypeTable,
					Strategy:    pipeline.MaterializationStrategyCreateReplace,
					PartitionBy: "dt",
				},
			},
			query: "SELECT 1",
			want:  "CREATE OR REPLACE TABLE `my.asset` PARTITION BY `dt`  AS\nSELECT 1",
		},
		{
			name: "materialize to a table with partition and cluster, single field to cluster",
			task: &pipeline.Task{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:        pipeline.MaterializationTypeTable,
					Strategy:    pipeline.MaterializationStrategyCreateReplace,
					PartitionBy: "dt",
					ClusterBy:   []string{"event_type"},
				},
			},
			query: "SELECT 1",
			want:  "CREATE OR REPLACE TABLE `my.asset` PARTITION BY `dt` CLUSTER BY `event_type` AS\nSELECT 1",
		},
		{
			name: "materialize to a table with partition and cluster, multiple fields to cluster",
			task: &pipeline.Task{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:        pipeline.MaterializationTypeTable,
					Strategy:    pipeline.MaterializationStrategyCreateReplace,
					PartitionBy: "dt",
					ClusterBy:   []string{"event_type", "event_name"},
				},
			},
			query: "SELECT 1",
			want:  "CREATE OR REPLACE TABLE `my.asset` PARTITION BY `dt` CLUSTER BY `event_type`, `event_name` AS\nSELECT 1",
		},
		{
			name: "materialize to a table with append",
			task: &pipeline.Task{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyAppend,
				},
			},
			query: "SELECT 1",
			want:  "INSERT INTO `my.asset` SELECT 1",
		},
		{
			name: "incremental strategies require the incremental_key to be set",
			task: &pipeline.Task{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyDeleteInsert,
				},
			},
			query:   "SELECT 1",
			wantErr: true,
		},
		{
			name: "incremental strategies require the incremental_key to be set",
			task: &pipeline.Task{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyDeleteInsert,
				},
			},
			query:   "SELECT 1",
			wantErr: true,
		},
		{
			name: "delete+insert builds a proper transaction",
			task: &pipeline.Task{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategyDeleteInsert,
					IncrementalKey: "dt",
				},
			},
			query: "SELECT 1",
			want: "BEGIN TRANSACTION\n" +
				"CREATE TEMP TABLE __blast_tmp AS SELECT 1\n" +
				"DELETE FROM `my.asset` WHERE `dt` in (SELECT DISTINCT `dt` FROM __blast_tmp)\n" +
				"INSERT INTO `my.asset` SELECT * FROM __blast_tmp\n" +
				"COMMIT TRANSACTION;",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := Materializer{}
			render, err := m.Render(tt.task, tt.query)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, tt.want, render)
		})
	}
}
