package query

import (
	"regexp"
	"strings"
)

type Renderer struct{}

var reIdentifiers = regexp.MustCompile(`(?s){{(([^}][^}]?|[^}]}?)*)}}`)

func (receiver Renderer) RenderQuery(query string, args map[string]string) (string, error) {
	matchedVariables := reIdentifiers.FindAllString(query, -1)
	if len(matchedVariables) == 0 {
		return query, nil
	}

	for _, variable := range matchedVariables {
		referencedRenderVariable := strings.Trim(variable[2:len(variable)-2], " ")
		if value, ok := args[referencedRenderVariable]; ok {
			query = strings.Replace(query, variable, value, -1)
		}
	}

	return query, nil
}
