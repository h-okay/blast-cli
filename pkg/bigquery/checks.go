package bigquery

import (
	"context"
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

func (n *NotNullCheck) Check(ctx context.Context, ti *scheduler.ColumnTestInstance) error {
	qq := fmt.Sprintf("SELECT count(*) FROM `%s` WHERE `%s` IS NULL", ti.GetAsset().Name, ti.Column.Name)
	res, err := n.q.Select(ctx, &query.Query{Query: qq})
	if err != nil {
		return errors.Wrap(err, "failed to check for null values during not_null check")
	}

	nullCount, err := ensureCountZero("not_null", res)
	if err != nil {
		return err
	}

	if nullCount != 0 {
		return errors.Errorf("column `%s` has %d null values", ti.Column.Name, nullCount)
	}

	return nil
}

type PositiveCheck struct {
	q querierWithResult
}

func (n *PositiveCheck) Check(ctx context.Context, ti *scheduler.ColumnTestInstance) error {
	qq := fmt.Sprintf("SELECT count(*) FROM `%s` WHERE `%s` <= 0", ti.GetAsset().Name, ti.Column.Name)
	res, err := n.q.Select(ctx, &query.Query{Query: qq})
	if err != nil {
		return errors.Wrap(err, "failed to check for null values during not_null check")
	}

	count, err := ensureCountZero("positive", res)
	if err != nil {
		return err
	}

	if count != 0 {
		return errors.Errorf("column `%s` has %d positive values", ti.Column.Name, count)
	}

	return nil
}
