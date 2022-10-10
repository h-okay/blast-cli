package query

import "github.com/flosch/pongo2/v6"

type JinjaRenderer struct {
	Context pongo2.Context
}

func (r *JinjaRenderer) Render(query string) string {
	tpl, err := pongo2.FromString(query)
	if err != nil {
		panic(err)
	}
	out, err := tpl.Execute(r.Context)
	if err != nil {
		panic(err)
	}
	return out
}
