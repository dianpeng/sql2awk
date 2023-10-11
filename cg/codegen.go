package cg

import (
	"fmt"
	"strings"
)

// codegen from plan to *awk* code. Notes, this pass does not generate sort
// instruction.

type CodeGen struct {
	Plan *plan.Plan
	buf  *strings.Builder
}

/**

BEGIN {
}

{
  // table scan ------------------------------------------------
}

END {
  join()
}

function join() {
  ...
}

function group_by_next(...) {
}

function group_by_flush() {
}

function group_by_done() {
}

function agg_next(...) {
}

function agg_flush() {
}

function agg_done() {
}

function having_next(...) {
}

function having_flush() {
}

function having_done() {
}

function output_next(...) {
}

function output_flush() {
}

function output_done() {
}

**/
