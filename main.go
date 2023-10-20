package main

import (
	"flag"
	"fmt"
	"github.com/dianpeng/sql2awk/cg"
	"github.com/dianpeng/sql2awk/plan"
	"github.com/dianpeng/sql2awk/sql"
	"io"
	"os"
)

var fOutput = flag.String(
	"output",
	"",
	"specify path to save output file, default write to STDOUT",
)

func oops(stage string, err error) {
	fmt.Fprintf(os.Stderr, "ERROR [%s]]] %s\n", stage, err)
	os.Exit(-1)
}

func readStdin() string {
	data, err := io.ReadAll(os.Stdin)
	if err != nil {
		oops("read sql", err)
	}
	return string(data)
}

func main() {
	flag.Parse()
	s := readStdin()

	parser := sql.NewParser(s)
	code, err := parser.Parse()
	if err != nil {
		oops("parse", err)
	}

	p, err := plan.PlanCode(code)
	if err != nil {
		oops("plan", err)
	}

	awkCode, err := cg.Generate(
		p,
		&cg.Config{
			OutputSeparator: " ",
		},
	)
	if err != nil {
		oops("code-gen", err)
	}

	if *fOutput == "" {
		fmt.Printf("%s\n", awkCode)
	} else {
		if err := os.WriteFile(
			*fOutput,
			[]byte(awkCode),
			0644,
		); err != nil {
			oops("save", err)
		}
	}
	os.Exit(0)
}
