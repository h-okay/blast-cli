package query

import (
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
)

func TestExtractAndRenderCouldWorkTogether(t *testing.T) {
	t.Parallel()

	query := `
-- @blast.name: some.task.name
-- @blast.type: sf.sql
-- @blast.depends: dependency-1

set variable1 = '{{ ds }}'::date;
set variable2 =  'some other date';
set variable3 = 21;


SELECT
    $variable1,
    $variable2,
    $variable3
;


set variable4 = dateadd(days, -($variable2 - 1), $variable1);
CREATE OR REPLACE TABLE my-awesome-table as
with dummy_dates as (
        SELECT
            dateadd(days, -(ROW_NUMBER() OVER (ORDER BY seq4()) - 1), $variable1) as event_date,
            concat(value1, '--', value2) as commentSyntaxAsString,
        FROM TABLE(GENERATOR(ROWCOUNT => $variable2 + 1))
    ),
    joinedTable as (
        SELECT
            field1,
			field2,
    	FROM dummy_dates
    ),
    secondTable as (SELECT 1),
    /*
    SELECT
		name, surname
    FROM my-multiline-comment-table
    GROUP BY 1,2
    ORDER BY 1,2;
     */

    SELECT
        a,
		b,
		c,

    FROM my-awesome-table
    GROUP BY 1,2,3
    ORDER BY 1, 2, 3
;
`

	expectedQueries := []*ExplainableQuery{
		{
			VariableDefinitions: []string{
				"set variable1 = '2022-01-01'::date",
				"set variable2 =  'some other date'",
				"set variable3 = 21",
			},
			Query: `SELECT
    $variable1,
    $variable2,
    $variable3`,
		},
		{
			VariableDefinitions: []string{
				"set variable1 = '2022-01-01'::date",
				"set variable2 =  'some other date'",
				"set variable3 = 21",
				"set variable4 = dateadd(days, -($variable2 - 1), $variable1)",
			},
			Query: `CREATE OR REPLACE TABLE my-awesome-table as
with dummy_dates as (
        SELECT
            dateadd(days, -(ROW_NUMBER() OVER (ORDER BY seq4()) - 1), $variable1) as event_date,
            concat(value1, '--', value2) as commentSyntaxAsString,
        FROM TABLE(GENERATOR(ROWCOUNT => $variable2 + 1))
    ),
    joinedTable as (
        SELECT
            field1,
			field2,
    	FROM dummy_dates
    ),
    secondTable as (SELECT 1),
    SELECT
        a,
		b,
		c,
    FROM my-awesome-table
    GROUP BY 1,2,3
    ORDER BY 1, 2, 3`,
		},
	}

	fs := afero.NewMemMapFs()
	err := afero.WriteFile(fs, "somefile.sql", []byte(query), 0o644)
	assert.NoError(t, err)

	extractor := FileExtractor{
		Fs: fs,
		Renderer: Renderer{
			Args: map[string]string{
				"ds": "2022-01-01",
			},
		},
	}
	res, err := extractor.ExtractQueriesFromFile("somefile.sql")
	assert.NoError(t, err)
	assert.Equal(t, expectedQueries, res)
}
