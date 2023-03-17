package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJinjaRenderer_RenderQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		query   string
		args    JinjaContext
		want    string
		wantErr bool
	}{
		{
			name:  "simple render for ds",
			query: "set analysis_end_date = '{{ ds }}'::date; select * from {{ ref('abc') }} and {{ utils.date_add('some-other') }} and {{ utils.multiparam('val1', 'val2') }}",
			args: JinjaContext{
				"ds": "2022-02-03",
				"ref": func(str string) string {
					return "some-ref-here"
				},
				"utils": map[string]any{
					"date_add": func(str string) string {
						return "some-date-here"
					},
					"multiparam": func(str1, str2 string) string {
						return str1 + "-" + str2
					},
				},
			},
			want: "set analysis_end_date = '2022-02-03'::date; select * from some-ref-here and some-date-here and val1-val2",
		},
		{
			name:  "multiple variables",
			query: "set analysis_end_date = '{{ ds }}'::date and '{{testVar}}' == 'testvar' and another date {{    ds }} - {{ someMissingVariable }};",
			args: JinjaContext{
				"ds":      "2022-02-03",
				"testVar": "testvar",
			},
			want: "set analysis_end_date = '2022-02-03'::date and 'testvar' == 'testvar' and another date 2022-02-03 - ;",
		},
		{
			name: "jinja variables work as well",
			query: `
{% set payment_method = "bank_transfer" %}

select
    order_id,
    sum(case when payment_method = '{{payment_method}}' then amount end) as {{payment_method}}_amount,
    sum(amount) as total_amount
from app_data.payments
group by 1
`,
			args: JinjaContext{},
			want: `


select
    order_id,
    sum(case when payment_method = 'bank_transfer' then amount end) as bank_transfer_amount,
    sum(amount) as total_amount
from app_data.payments
group by 1`,
		},
		{
			name: "given array from outside is rendered",
			query: `
select
    order_id,
    {% for payment_method in payment_methods %}
    sum(case when payment_method = '{{payment_method}}' then amount end) as {{payment_method}}_amount,
    {% endfor %}
    sum(amount) as total_amount
from app_data.payments
group by 1`,
			args: JinjaContext{
				"payment_methods": []string{"bank_transfer", "credit_card", "gift_card"},
			},
			want: `
select
    order_id,
    
    sum(case when payment_method = 'bank_transfer' then amount end) as bank_transfer_amount,
    
    sum(case when payment_method = 'credit_card' then amount end) as credit_card_amount,
    
    sum(case when payment_method = 'gift_card' then amount end) as gift_card_amount,
    
    sum(amount) as total_amount
from app_data.payments
group by 1`,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			receiver := NewJinjaRenderer(tt.args)
			got := receiver.Render(tt.query)

			require.Equal(t, tt.want, got)
		})
	}
}

func Test_DateAdd(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		args     []interface{}
		expected string
		wantErr  bool
	}{
		{
			name:     "add 5 days to a date string with default output format",
			args:     []interface{}{"2022-12-31", 5},
			expected: "2023-01-05",
		},
		{
			name:     "add 10 days to a date string with custom output format",
			args:     []interface{}{"2022-12-31", 10, "2006/01/02"},
			expected: "2023/01/10",
		},
		{
			name:     "add -3 days to a datetime string with custom input and output formats",
			args:     []interface{}{"2022-12-31 12:34:56", -3, "02/01/06 15:04:05", "2006-01-02 15:04:05"},
			expected: "28/12/22 12:34:56",
		},
		{
			name:     "invalid arguments - fewer than 2",
			args:     []interface{}{},
			expected: "invalid arguments for date_add",
		},
		{
			name:     "invalid arguments - date format",
			args:     []interface{}{"12/31/2022", 10},
			expected: "invalid date format:12/31/2022",
		},
		{
			name:     "invalid arguments - output format",
			args:     []interface{}{"2022-12-31", 10, 123},
			expected: "invalid output format",
		},
	}

	for _, tc := range testCases {
		tt := tc
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			actualDate := dateAdd(tt.args...)
			assert.Equal(t, tt.expected, actualDate)
		})
	}
}
