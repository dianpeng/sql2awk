package cg

import (
	_ "embed"
)

//go:embed builtin.awk
var builtinAWK string

//go:embed builtin-goawk.awk
var builtinGoAWK string
