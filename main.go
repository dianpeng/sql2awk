package main

import (
	"fmt"
	"github.com/dianpeng/sql2awk/cg"
	"github.com/dianpeng/sql2awk/plan"
	"github.com/dianpeng/sql2awk/sql"
	"io"
	"os"
)

func oops(stage string, err error) {
	fmt.Fprintf(os.Stderr, "[%s]: %s", stage, err)
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
	s := readStdin()
	parser := sql.NewParser(s)
	code, err := parser.Parse()
	if err != nil {
		oops("parse", err)
	}
	p := plan.PlanCode(code)
	if err != nil {
		oops("plan", err)
	}
	awkCode, err := cg.Generate(
		p,
		&cg.Config{
			OutputSeparator: " ",
		},
	)

	fmt.Printf("%s\n", awkCode)
	os.Exit(0)
}
