package query

import (
	"fmt"
	"sync"
	"time"

	"github.com/datablast-analytics/blast-cli/pkg/date"
	"github.com/noirbizarre/gonja"
)

type JinjaRenderer struct {
	context         gonja.Context
	queryRenderLock *sync.Mutex
}

type JinjaContext map[string]any

func NewJinjaRenderer(context JinjaContext) *JinjaRenderer {
	return &JinjaRenderer{
		context:         gonja.Context(context),
		queryRenderLock: &sync.Mutex{},
	}
}

// this func uses variadic arguments because I couldn't find a nicer way of supporting optional arguments
// if anyone has a better idea that won't break the API, I am open for contributions.
func dateAdd(args ...interface{}) string {
	if len(args) < 2 {
		return "invalid arguments for date_add"
	}

	inputFormat := ""
	if len(args) > 3 {
		inputFormatCasted, ok := args[3].(string)
		if !ok {
			return "invalid input format"
		}
		inputFormat = date.ConvertPythonDateFormatToGolang(inputFormatCasted)
	}

	var inputDate time.Time
	var err error

	inputDateString, ok := args[0].(string)
	if !ok {
		return "invalid date"
	}

	if inputFormat == "" {
		inputDate, err = date.ParseTime(inputDateString)
		if err != nil {
			return fmt.Sprintf("invalid date format:%s", inputDateString)
		}
	} else {
		inputDate, err = time.Parse(inputFormat, inputDateString)
		if err != nil {
			return fmt.Sprintf("invalid date format:%s", inputDateString)
		}
	}

	days, ok := args[1].(int)
	if !ok {
		return "invalid days for date_add"
	}

	outputFormat := "2006-01-02"
	if len(args) > 2 {
		outputFormatString, ok := args[2].(string)
		if !ok {
			return "invalid output format"
		}

		outputFormat = date.ConvertPythonDateFormatToGolang(outputFormatString)
	}

	format := inputDate.AddDate(0, 0, days).Format(outputFormat)
	return format
}

func NewJinjaRendererFromStartEndDates(startDate, endDate *time.Time) *JinjaRenderer {
	ctx := gonja.Context{
		"start_date":             startDate.Format("2006-01-02"),
		"start_date_nodash":      startDate.Format("20060102"),
		"start_datetime":         startDate.Format("2006-01-02T15:04:05"),
		"start_datetime_with_tz": startDate.Format(time.RFC3339),
		"end_date":               endDate.Format("2006-01-02"),
		"end_date_nodash":        endDate.Format("20060102"),
		"end_datetime":           endDate.Format("2006-01-02T15:04:05"),
		"end_datetime_with_tz":   endDate.Format(time.RFC3339),

		"utils": map[string]interface{}{
			"date_add": dateAdd,
			"date_format": func(str, inputFormat, outputFormat string) string {
				return str
			},
		},
	}

	return &JinjaRenderer{
		context:         ctx,
		queryRenderLock: &sync.Mutex{},
	}
}

func (r *JinjaRenderer) Render(query string) string {
	r.queryRenderLock.Lock()

	tpl, err := gonja.FromString(query)
	if err != nil {
		panic(err)
	}
	r.queryRenderLock.Unlock()

	// Now you can render the template with the given
	// gonja.context how often you want to.
	out, err := tpl.Execute(r.context)
	if err != nil {
		panic(err)
	}

	return out
}
