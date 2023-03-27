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

func (n NotNullCheck) Check(ctx context.Context, ti *scheduler.ColumnTestInstance) error {
	qq := fmt.Sprintf("SELECT count(*) FROM `%s` WHERE `%s` IS NULL", ti.GetAsset().Name, ti.Column.Name)
	res, err := n.q.Select(ctx, &query.Query{Query: qq})
	if err != nil {
		return errors.Wrap(err, "failed to check for null values during not_null check")
	}

	if len(res) != 1 || len(res[0]) != 1 {
		return errors.New("unexpected result from query during not_null check")
	}

	nullCount, ok := res[0][0].(int64)
	if !ok {
		nullCountInt, ok := res[0][0].(int)
		if !ok {
			return errors.New("unexpected result from query during not_null check, cannot cast result to integer")
		}

		nullCount = int64(nullCountInt)
	}

	if nullCount != 0 {
		return errors.Errorf("column `%s` has %d null values", ti.Column.Name, nullCount)
	}

	return nil
}
