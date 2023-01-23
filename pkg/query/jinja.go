package query

import (
	"sync"

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
