package query

import (
	"regexp"
	"strings"
	"time"
)

var DefaultRenderer = &Renderer{
	Args: map[string]string{
		"ds":                   time.Now().Format("2006-01-02"),
		"ds_nodash":            time.Now().Format("20060102"),
		"macros.ds_add(ds, 1)": time.Now().Add(24 * time.Hour).Format("2006-01-02"),
	},
}

type Renderer struct {
	Args map[string]string
}

var reIdentifiers = regexp.MustCompile(`(?s){{(([^}][^}]?|[^}]}?)*)}}`)

func (r Renderer) Render(query string) string {
	matchedVariables := reIdentifiers.FindAllString(query, -1)
	if len(matchedVariables) == 0 {
		return query
	}

	for _, variable := range matchedVariables {
		referencedRenderVariable := strings.Trim(variable[2:len(variable)-2], " ")
		if value, ok := r.Args[referencedRenderVariable]; ok {
			query = strings.ReplaceAll(query, variable, value)
		}
	}

	return query
}
