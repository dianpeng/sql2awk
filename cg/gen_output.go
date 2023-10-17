package cg

import (
	"fmt"
	"github.com/dianpeng/sql2awk/plan"
	"strings"
)

func generateOutputPrologue(
	cg *queryCodeGen,
	writer *awkWriter,
) {
	writer.Line("$[g, output_count]++;", nil)
}

type outputCodeGen struct {
	cg       *queryCodeGen
	wildcard outputCodeGenWildcard
	normal   outputCodeGenNormal
}

func newOutputCodeGen(
	cg *queryCodeGen,
) *outputCodeGen {
	return &outputCodeGen{
		cg: cg,
		wildcard: outputCodeGenWildcard{
			cg: cg,
		},
		normal: outputCodeGenNormal{
			cg: cg,
		},
	}
}

func (self *outputCodeGen) isWildcard() bool {
	return self.cg.query.Output.Wildcard
}

func (self *outputCodeGen) setWriter(w *awkWriter) {
	if self.isWildcard() {
		self.wildcard.writer = w
	} else {
		self.normal.writer = w
	}
}

func (self *outputCodeGen) genNext() error {
	if self.isWildcard() {
		return self.wildcard.genNext(self.cg.query.Output)
	} else {
		return self.normal.genNext(self.cg.query.Output)
	}
}

func (self *outputCodeGen) genFlush() error {
	if self.isWildcard() {
		return self.wildcard.genFlush()
	} else {
		return self.normal.genFlush()
	}
}

func (self *outputCodeGen) genDone() error {
	if self.isWildcard() {
		return self.wildcard.genDone()
	} else {
		return self.normal.genDone()
	}
}

/* ------------------------------------------------------------------------
 * Wildcard
 * ----------------------------------------------------------------------*/

type outputCodeGenWildcard struct {
	cg     *queryCodeGen
	writer *awkWriter
}

func (self *outputCodeGenWildcard) genVarOutput(plan *plan.Output) error {
	p := self.cg.query

	for _, ts := range p.TableScan {
		self.writer.Chunk(
			`
for ($[l, i] = 1; $[l, i] <= %[table_size]; $[l, i]++) {
  printf "%s%[sep]",%[table][%[rid], $[l, i]]
}
`,
			awkWriterCtx{
				"table":      self.cg.varTable(ts.Table.Index),
				"table_size": self.cg.varTableField(ts.Table.Index),
				"rid":        self.cg.varRID(ts.Table.Index),
				"sep":        self.cg.OutputSeparator,
			},
		)
	}
	return nil
}

func (self *outputCodeGenWildcard) genOutput(output *plan.Output) error {
	generateOutputPrologue(self.cg, self.writer)

	if err := self.genVarOutput(output); err != nil {
		return err
	}
	self.writer.Line("print \"\"", nil)
	return nil
}

// distinct for wildcard, easy just setup a distinct table
func (self *outputCodeGenWildcard) genDistinct(output *plan.Output) error {
	if !output.Distinct {
		return nil
	}

	p := self.cg.query

	self.writer.DefineLocal(
		"distinct_key",
	)
	for _, ts := range p.TableScan {
		self.writer.Chunk(
			`
for ($[l, i] = 1; $[l, i] <= %[table_size]; $[l, i]++) {
  $[l, distinct_key] = sprintf("%s%s", $[l, distinct_key], %[table][%[rid], $[l, i]]);
}
`,
			awkWriterCtx{
				"table":      self.cg.varTable(ts.Table.Index),
				"table_size": self.cg.varTableField(ts.Table.Index),
				"rid":        self.cg.varRID(ts.Table.Index),
				"sep":        self.cg.OutputSeparator,
			},
		)
	}

	// okay, now use the distinct key to check distinct table
	self.writer.Chunk(
		`
  if ($[ga, distinct][$[l, distinct_key]] == "") {
    $[ga, distinct][$[l, distinct_key]] = "Y";
  } else {
    return;
  }
`,
		nil,
	)
	return nil
}

func (self *outputCodeGenWildcard) genLimit(output *plan.Output) error {
	if !output.HasLimit() {
		return nil
	}

	self.writer.Chunk(
		`
if ($[g, output_count] >= %[limit]) {
  return;
}
`,
		awkWriterCtx{
			"limit": output.Limit,
		},
	)
	return nil
}

func (self *outputCodeGenWildcard) genNext(output *plan.Output) error {
	if err := self.genLimit(output); err != nil {
		return err
	}
	if err := self.genDistinct(output); err != nil {
		return err
	}
	if err := self.genOutput(output); err != nil {
		return err
	}
	return nil
}

func (self *outputCodeGenWildcard) genFlush() error {
	return nil
}

func (self *outputCodeGenWildcard) genDone() error {
	return nil
}

/* ------------------------------------------------------------------------
 * Normal expression
 * ----------------------------------------------------------------------*/

type outputCodeGenNormal struct {
	cg     *queryCodeGen
	writer *awkWriter
}

func (self *outputCodeGenNormal) setup(output *plan.Output) {
	self.writer.DefineLocal("distinct_key")

	for idx, _ := range output.VarList {
		self.writer.DefineLocalN("output_val", idx)
	}
}

func (self *outputCodeGenNormal) genLimit(
	output *plan.Output,
) error {
	if !output.HasLimit() {
		return nil
	}
	self.writer.Chunk(
		`
if ($[g, output_count] >= %[limit]) {
  return;
}
`,
		awkWriterCtx{
			"limit": output.Limit,
		},
	)
	return nil
}

func (self *outputCodeGenNormal) genCalc(
	output *plan.Output,
) {
	for idx, expr := range output.VarList {
		xx := self.cg.genExpr(expr)
		self.writer.Assign(
			self.writer.LocalN("output_val", idx),
			fmt.Sprintf("(%s\"\")", xx),
			nil,
		)
	}
}

func (self *outputCodeGenNormal) outputLocalList(output *plan.Output) []string {
	x := []string{}
	for idx, _ := range output.VarList {
		x = append(x, self.writer.LocalN("output_val", idx))
	}
	return x
}

func (self *outputCodeGenNormal) genDistinct(
	output *plan.Output,
) error {
	if !output.Distinct {
		return nil
	}

	for idx, _ := range output.VarList {
		self.writer.Line(
			`$[l, distinct_key] = sprintf("%s%s", $[l, distinct_key], %[oval]);`,
			awkWriterCtx{
				"oval": self.writer.LocalN("output_val", idx),
			},
		)
	}

	// okay, now use the distinct key to check distinct table
	self.writer.Chunk(
		`
if ($[ga, distinct][$[l, distinct_key]] == "") {
  $[ga, distinct][$[l, distinct_key]] = "Y";
} else {
  return;
}
`,
		nil,
	)
	return nil
}

func (self *outputCodeGenNormal) genVarOutput(
	output *plan.Output,
) error {
	self.writer.oIndent()
	self.writer.o("printf \"")
	for _, _ = range output.VarList {
		self.writer.o("%s")
		self.writer.o(self.cg.OutputSeparator)
	}
	self.writer.o("\",")

	x := self.outputLocalList(output)
	self.writer.o(strings.Join(x, ","))
	self.writer.o(";")
	self.writer.oLB()
	return nil
}

func (self *outputCodeGenNormal) genOutput(
	output *plan.Output,
) error {
	generateOutputPrologue(self.cg, self.writer)
	self.genCalc(output)

	// distinct must be placed *after* the evaluation
	if err := self.genDistinct(output); err != nil {
		return err
	}

	if err := self.genVarOutput(output); err != nil {
		return err
	}
	self.writer.Line("print \"\"", nil)
	return nil
}

func (self *outputCodeGenNormal) genNext(output *plan.Output) error {
	if err := self.genLimit(output); err != nil {
		return err
	}
	if err := self.genOutput(output); err != nil {
		return err
	}
	return nil
}

func (self *outputCodeGenNormal) genFlush() error {
	return nil
}

func (self *outputCodeGenNormal) genDone() error {
	return nil
}
