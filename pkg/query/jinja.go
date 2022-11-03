package query

import (
	"sync"

	"github.com/flosch/pongo2/v6"
)

type JinjaRenderer struct {
	Context pongo2.Context

	set             *pongo2.TemplateSet
	queryRenderLock *sync.Mutex
}

func NewJinjaRenderer(context pongo2.Context) *JinjaRenderer {
	sandboxedLoader, err := pongo2.NewSandboxedFilesystemLoader(".")
	if err != nil {
		panic(err)
	}

	return &JinjaRenderer{
		Context: context,

		set:             pongo2.NewSet("query", sandboxedLoader),
		queryRenderLock: &sync.Mutex{},
	}
}

func (r *JinjaRenderer) Render(query string) string {
	r.queryRenderLock.Lock()
	tpl, err := r.set.FromString(query)
	r.queryRenderLock.Unlock()

	if err != nil {
		panic(err)
	}

	out, err := tpl.Execute(r.Context)
	if err != nil {
		panic(err)
	}
	return out
}
