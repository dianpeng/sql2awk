package cg

import (
	"github.com/dianpeng/sql2awk/plan"
	_ "strings"
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
	f := self.cg.query.Format

	if f.IsColumnFormatDefault() {
		// fastpath, do not need to implement dynamic formatting of wildcard
		for _, ts := range p.TableScan {
			self.writer.Chunk(
				`
  for ($[l, i] = 1; $[l, i] <= %[table_size]; $[l, i]++) {
    printf("%[sep]%-%[padding]s",%[table][%[rid], $[l, i]]);
  }
  `,
				awkWriterCtx{
					"padding":    self.cg.formatPaddingSize(),
					"table":      self.cg.varTable(ts.Table.Index),
					"table_size": self.cg.varTableField(ts.Table.Index),
					"rid":        self.cg.varRID(ts.Table.Index),
					"sep":        self.cg.formatSep(),
				},
			)
		}
	} else {
		for _, ts := range p.TableScan {
			self.writer.Chunk(
				`
  for ($[l, i] = 1; $[l, i] <= %[table_size]; $[l, i]++) {
    format_wildcard_print_column($[l, i]-1, %[table][%[rid], $[l, i]]);
  }
  `,
				awkWriterCtx{
					"table":      self.cg.varTable(ts.Table.Index),
					"table_size": self.cg.varTableField(ts.Table.Index),
					"rid":        self.cg.varRID(ts.Table.Index),
				},
			)
		}
	}

	self.writer.Line(
		`printf("%[sep]\n");`,
		awkWriterCtx{
			"sep": self.cg.formatSep(),
		},
	)
	return nil
}

func (self *outputCodeGenWildcard) genOutput(output *plan.Output) error {
	generateOutputPrologue(self.cg, self.writer)

	if err := self.genVarOutput(output); err != nil {
		return err
	}
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
	for idx, ovar := range output.VarList {
		switch ovar.Type {
		case plan.OutputVarWildcard, plan.OutputVarRowMatch, plan.OutputVarColMatch:
			if output.Distinct {
				// if we have distinct, we do the dumpping, otherwise do nothing here
				// since we cannot save wildcard column for this table into local
				// variables, it is a special case
				self.writer.Chunk(
					`
%[output] = "";
for ($[l, idx] = 1; $[l, idx] <= %[table_fnum]; ++$[l, idx]) {
  %[output] = sprintf("%s%s", $[output], %[table][%[rid], $[l, idx]]);
}
`,
					awkWriterCtx{
						"output":     self.writer.LocalN("output_val", idx),
						"table_fnum": self.cg.varTableField(ovar.Table.Index),
						"table":      self.cg.varTable(ovar.Table.Index),
						"rid":        self.writer.rid(ovar.Table.Index),
					},
				)
			}
			break

		default:
			xx := self.cg.genExpr(ovar.Value)
			self.writer.Assign(
				self.writer.LocalN("output_val", idx),
				xx,
				nil,
			)
			break
		}
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
	if !output.HasTableWildcard() {
		self.writer.Call(
			"format_next",
			self.outputLocalList(output),
		)
	} else {
		self.writer.Line("$[l, cidx] = 0;", nil)

		for idx, ovar := range output.VarList {
			switch ovar.Type {
			case plan.OutputVarWildcard, plan.OutputVarRowMatch, plan.OutputVarColMatch:
				self.writer.Chunk(
					`
for ($[l, idx] = 1; $[l, idx] <= %[table_fnum]; ++$[l, idx]) {
  format_wildcard_print_column($[l, cidx], %[table][%[rid], $[l, idx]]);
  $[l, cidx]++;
}
`,
					awkWriterCtx{
						"table_fnum": self.cg.varTableField(ovar.Table.Index),
						"table":      self.cg.varTable(ovar.Table.Index),
						"rid":        self.writer.rid(ovar.Table.Index),
					},
				)
				break
			default:
				self.writer.Line(
					`format_wildcard_print_column($[l, cidx], %[tmp]);`,
					awkWriterCtx{
						"tmp": self.writer.LocalN("output_val", idx),
					},
				)
				self.writer.Line("$[l, cidx]++;", nil)
				break
			}
		}
		self.writer.Line(
			`printf("%[sep]\n");`,
			awkWriterCtx{
				"sep": self.cg.formatSep(),
			},
		)
	}
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
