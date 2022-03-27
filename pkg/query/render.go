package query

import (
	"regexp"
	"strings"
)

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
