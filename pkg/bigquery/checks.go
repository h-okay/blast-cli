package bigquery

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/datablast-analytics/blast/pkg/query"
	"github.com/datablast-analytics/blast/pkg/scheduler"
	"github.com/pkg/errors"
)

type querierWithResult interface {
	Select(ctx context.Context, q *query.Query) ([][]interface{}, error)
}

type NotNullCheck struct {
	q querierWithResult
}

func ensureCountZero(check string, res [][]interface{}) (int64, error) {
	if len(res) != 1 || len(res[0]) != 1 {
		return 0, errors.Errorf("unexpected result from query during %s check", check)
	}

	nullCount, ok := res[0][0].(int64)
	if !ok {
		nullCountInt, ok := res[0][0].(int)
		if !ok {
			return 0, errors.Errorf("unexpected result from query during %s check, cannot cast result to integer", check)
		}

		nullCount = int64(nullCountInt)
	}

	return nullCount, nil
}

func (c *NotNullCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	qq := fmt.Sprintf("SELECT count(*) FROM `%s` WHERE `%s` IS NULL", ti.GetAsset().Name, ti.Column.Name)

	return (&countZeroCheck{
		q:             c.q,
		queryInstance: &query.Query{Query: qq},
		checkName:     "not_null",
		customError: func(count int64) error {
			return errors.Errorf("column `%s` has %d null values", ti.Column.Name, count)
		},
	}).Check(ctx, ti)
}

type PositiveCheck struct {
	q querierWithResult
}

func (c *PositiveCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	qq := fmt.Sprintf("SELECT count(*) FROM `%s` WHERE `%s` <= 0", ti.GetAsset().Name, ti.Column.Name)
	return (&countZeroCheck{
		q:             c.q,
		queryInstance: &query.Query{Query: qq},
		checkName:     "positive",
		customError: func(count int64) error {
			return errors.Errorf("column `%s` has %d non-positive values", ti.Column.Name, count)
		},
	}).Check(ctx, ti)
}

type UniqueCheck struct {
	q querierWithResult
}

func (c *UniqueCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	qq := fmt.Sprintf("SELECT COUNT(`%s`) - COUNT(DISTINCT `%s`) FROM `%s`", ti.Column.Name, ti.Column.Name, ti.GetAsset().Name)
	return (&countZeroCheck{
		q:             c.q,
		queryInstance: &query.Query{Query: qq},
		checkName:     "unique",
		customError: func(count int64) error {
			return errors.Errorf("column `%s` has %d non-unique values", ti.Column.Name, count)
		},
	}).Check(ctx, ti)
}

type AcceptedValuesCheck struct {
	q querierWithResult
}

func (c *AcceptedValuesCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	if ti.Check.Value.StringArray == nil && ti.Check.Value.IntArray == nil {
		return errors.Errorf("unexpected value for accepted_values check, the values must to be an array, instead %T", ti.Check.Value)
	}

	if ti.Check.Value.StringArray != nil && len(*ti.Check.Value.StringArray) == 0 {
		return errors.Errorf("no values provided for accepted_values check")
	}

	if ti.Check.Value.IntArray != nil && len(*ti.Check.Value.IntArray) == 0 {
		return errors.Errorf("no values provided for accepted_values check")
	}

	var val []string
	if ti.Check.Value.StringArray != nil {
		val = *ti.Check.Value.StringArray
	} else {
		for _, v := range *ti.Check.Value.IntArray {
			val = append(val, fmt.Sprintf("%d", v))
		}
	}

	res, err := json.Marshal(val)
	if err != nil {
		return errors.Wrap(err, "failed to marshal accepted values for the query result")
	}

	sz := len(res)
	res = res[1 : sz-1]

	qq := fmt.Sprintf("SELECT COUNT(*) FROM `%s` WHERE CAST(`%s` as STRING) NOT IN (%s)", ti.GetAsset().Name, ti.Column.Name, res)
	return (&countZeroCheck{
		q:             c.q,
		queryInstance: &query.Query{Query: qq},
		checkName:     "accepted_values",
		customError: func(count int64) error {
			return errors.Errorf("column `%s` has %d rows that are not in the accepted values", ti.Column.Name, count)
		},
	}).Check(ctx, ti)
}

type countZeroCheck struct {
	q             querierWithResult
	queryInstance *query.Query
	checkName     string
	customError   func(count int64) error
}

func (c *countZeroCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	res, err := c.q.Select(ctx, c.queryInstance)
	if err != nil {
		return errors.Wrapf(err, "failed '%s' check", c.checkName)
	}

	count, err := ensureCountZero(c.checkName, res)
	if err != nil {
		return err
	}

	if count != 0 {
		return c.customError(count)
		// return errors.Errorf("column `%s` has %d positive values", ti.Column.Name, count)
	}

	return nil
}
