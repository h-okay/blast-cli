package main

import (
	"os"
	"time"

	"github.com/datablast-analytics/blast/cmd"
	"github.com/fatih/color"
	"github.com/urfave/cli/v2"
)

func main() {
	isDebug := false
	color.NoColor = false

	app := &cli.App{
		Name:     "blast",
		Usage:    "The CLI used for managing Blast-powered data pipelines",
		Compiled: time.Now(),
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:        "debug",
				Value:       false,
				Usage:       "show debug information",
				Destination: &isDebug,
			},
		},
		Commands: []*cli.Command{
			cmd.Lint(&isDebug),
			cmd.Run(&isDebug),
			cmd.Render(),
			cmd.Lineage(),
		},
	}

	_ = app.Run(os.Args)
}
