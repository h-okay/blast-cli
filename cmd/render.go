package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/alecthomas/chroma/v2/quick"
	"github.com/datablast-analytics/blast/pkg/bigquery"
	"github.com/datablast-analytics/blast/pkg/executor"
	"github.com/datablast-analytics/blast/pkg/query"
	"github.com/urfave/cli/v2"
)

func Render() *cli.Command {
	return &cli.Command{
		Name:      "render",
		Usage:     "render a single Blast SQL asset",
		ArgsUsage: "[path to the asset file]",
		Action: func(c *cli.Context) error {
			taskPath := c.Args().Get(0)
			if taskPath == "" {
				errorPrinter.Printf("Please give an asset path to render: blast-cli render <path to the asset file>)\n")
				return cli.Exit("", 1)
			}

			task, err := builder.CreateTaskFromFile(taskPath)
			if err != nil {
				errorPrinter.Printf("Failed to build asset: %v\n", err.Error())
				return cli.Exit("", 1)
			}

			if task == nil {
				errorPrinter.Printf("The given file path doesn't seem to be a Blast asset definition: '%s'\n", taskPath)
				return cli.Exit("", 1)
			}

			wholeFileExtractor := &query.WholeFileExtractor{
				Fs:       fs,
				Renderer: query.DefaultJinjaRenderer,
			}

			queries, err := wholeFileExtractor.ExtractQueriesFromFile(task.ExecutableFile.Path)
			if err != nil {
				errorPrinter.Printf("Failed to extract queries from file: %v\n", err.Error())
				return cli.Exit("", 1)
			}

			qq := queries[0]

			if task.Type == executor.TaskTypeBigqueryQuery {
				materializer := bigquery.Materializer{}
				materialized, err := materializer.Render(task, qq.Query)
				if err != nil {
					errorPrinter.Printf("Failed to materialize the query: %v\n", err.Error())
					return cli.Exit("", 1)
				}

				qq.Query = materialized
				qq.Query = highlightCode(qq.Query, "sql")
			}

			fmt.Printf("%s\n", qq)

			return nil
		},
	}
}

func highlightCode(code string, language string) string {
	o, _ := os.Stdout.Stat()
	if (o.Mode() & os.ModeCharDevice) != os.ModeCharDevice {
		return code
	}
	b := new(strings.Builder)
	err := quick.Highlight(b, code, language, "terminal16m", "monokai")
	if err != nil {
		errorPrinter.Printf("Failed to highlight the query: %v\n", err.Error())
		return code
	}

	return b.String()
}
