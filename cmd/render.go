package cmd

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/alecthomas/chroma/v2/quick"
	"github.com/datablast-analytics/blast/pkg/bigquery"
	"github.com/datablast-analytics/blast/pkg/executor"
	"github.com/datablast-analytics/blast/pkg/pipeline"
	"github.com/datablast-analytics/blast/pkg/query"
	"github.com/urfave/cli/v2"
)

func Render() *cli.Command {
	return &cli.Command{
		Name:      "render",
		Usage:     "render a single Blast SQL asset",
		ArgsUsage: "[path to the asset definition]",
		Action: func(c *cli.Context) error {
			r := RenderCommand{
				extractor: &query.WholeFileExtractor{
					Fs:       fs,
					Renderer: query.DefaultJinjaRenderer,
				},
				bqMaterializer: &bigquery.Materializer{},
				builder:        builder,
				writer:         os.Stdout,
			}

			return r.Run(c.Args().Get(0))
		},
	}
}

type queryExtractor interface {
	ExtractQueriesFromFile(path string) ([]*query.Query, error)
}

type queryMaterializer interface {
	Render(task *pipeline.Asset, query string) (string, error)
}

type taskCreator interface {
	CreateTaskFromFile(path string) (*pipeline.Asset, error)
}

type RenderCommand struct {
	extractor      queryExtractor
	bqMaterializer queryMaterializer
	builder        taskCreator

	writer io.Writer
}

func (r *RenderCommand) Run(taskPath string) error {
	if taskPath == "" {
		errorPrinter.Printf("Please give an asset path to render: blast-cli render <path to the asset file>)\n")
		return cli.Exit("", 1)
	}

	task, err := r.builder.CreateTaskFromFile(taskPath)
	if err != nil {
		errorPrinter.Printf("Failed to build asset: %v\n", err.Error())
		return cli.Exit("", 1)
	}

	if task == nil {
		errorPrinter.Printf("The given file path doesn't seem to be a Blast asset definition: '%s'\n", taskPath)
		return cli.Exit("", 1)
	}

	queries, err := r.extractor.ExtractQueriesFromFile(task.ExecutableFile.Path)
	if err != nil {
		errorPrinter.Printf("Failed to extract queries from file: %v\n", err.Error())
		return cli.Exit("", 1)
	}

	qq := queries[0]

	if task.Type == executor.TaskTypeBigqueryQuery {
		materialized, err := r.bqMaterializer.Render(task, qq.Query)
		if err != nil {
			errorPrinter.Printf("Failed to materialize the query: %v\n", err.Error())
			return cli.Exit("", 1)
		}

		qq.Query = materialized
		qq.Query = highlightCode(qq.Query, "sql")
	}

	_, err = r.writer.Write([]byte(fmt.Sprintf("%s\n", qq)))

	return err
}

func highlightCode(code string, language string) string {
	o, err := os.Stdout.Stat()
	if err != nil {
		return code
	}

	if (o.Mode() & os.ModeCharDevice) != os.ModeCharDevice {
		return code
	}
	b := new(strings.Builder)
	err = quick.Highlight(b, code, language, "terminal16m", "monokai")
	if err != nil {
		errorPrinter.Printf("Failed to highlight the query: %v\n", err.Error())
		return code
	}

	return b.String()
}
