package cg

import (
	_ "embed"
	"fmt"
)

//go:embed awk/builtin.awk
var builtinAWK string

//go:embed awk/xsv.awk
var builtinAWKCSV string

//go:embed awk/base64.awk
var builtinAWKBase64 string

//go:embed goawk/builtin.awk
var builtinGoAWK string

func builtin() string {
	return fmt.Sprintf(
		"%s\n%s\n%s\n",
		builtinAWK,
		builtinAWKCSV,
		builtinAWKBase64,
	)
}
