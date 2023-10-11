package cg

func generateSortOutput(
	cg *CodeGen,
	writer *awkWriter,
	sortList []sql.Expr,
) error {
	for _, x := range sortList {
		if expr, err := cg.genExpr(x); err != nil {
			return err
		} else {
			writer.Line(
				"printf \"%s%[sep]\",(%[value])",
				map[string]interface{}{
					"sep":   cg.OutputSeparator,
					"value": expr,
				},
			)
		}
	}
	return nil
}

/* ------------------------------------------------------------------------
 * Wildcard
 * ----------------------------------------------------------------------*/

type outputCodeGenWildcard struct {
	cg     *CodeGen
	writer *awkWriter
}

func (self *outputCodeGenWildcard) genVarOutput(plan *plan.Output) error {
	p := self.cg.p

	for _, ts := range p.TableScan {
		self.writer.Chunk(
			`
for (i = 0; i < %[table_size]; i++) {
  printf "%s%[sep]",%[table][i]
}
`,
			map[string]interface{}{
				"table":      self.cg.varTable(ts.Table.Index),
				"table_size": self.cg.varTableSize(ts.Table.Index),
				"sep":        self.cg.OutputSeperator,
			},
		)
	}
	return nil
}

func (self *outputCodeGenWildcard) genSortOutput(output *plan.Output) error {
	return generateSortOutput(plan.SortList)
}

func (self *outputCodeGenWildcard) genOutput(output *plan.Output) error {
	self.writer.Line("output_count++;")

	if err := self.genVarOutput(output); err != nil {
		return err
	}
	if err := self.genSortOutput(output); err != nil {
		return err
	}
	self.writer.Line("print \"\"")
	return nil
}

// distinct for wildcard, easy just setup a distinct table
func (self *outputCodeGenWildcard) genDistinct(output *plan.Output) error {
	if output.Distinct {
		return nil
	}

	p := self.cg.p

	self.writer.DefineLocal(
		"distinct_key",
		"\"\"",
	)
	for _, ts := range p.TableScan {
		self.writer.Chunk(
			`
for (i = 0; i < %[table_size]; i++) {
  distinct_key += (%[table][i]+'')
}
`,
			map[string]interface{}{
				"table":      self.cg.varTable(ts.Table.Index),
				"table_size": self.cg.varTableSize(ts.Table.Index),
				"sep":        self.cg.OutputSeperator,
			},
		)
	}

	// okay, now use the distinct key to check distinct table
	self.writer.Chunk(
		`
  if (distinct[distinct_key] == "") {
    distinct[distinct_key] = "Y";
  } else {
    return;
  }
`,
		nil,
	)
}

func (self *outputCodeGenWildcard) genLimit(output *plan.Output) error {
	if !output.HasLimit() {
		return nil
	}

	self.writer.Chunk(
		`
if (output_count >= %[limit]) {
  return;
}
`,
		map[string]interface{}{
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
	cg     *CodeGen
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
if (output_count >= %[limit]) {
  return;
}
`,
		map[string]interface{}{
			"limit": output.Limit,
		},
	)
	return nil
}

func (self *outputCodeGenNormal) genCalc(
	output *plan.Output,
) {
	for idx, expr := range output.VarList {
		if xx, err := self.cg.genExpr(expr); err != nil {
			return err
		} else {
			self.writer.Assign(
				self.writer.LocalN("output_val", idx),
				fmt.Sprintf("(%s+\"\")", xx),
			)
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
	x := self.outputLocalList(output)

	self.writer.Assign(
		"distinct_key",
		strings.Join(x, "+"),
	)

	// okay, now use the distinct key to check distinct table
	self.writer.Chunk(
		`
  if (distinct[distinct_key] == "") {
    distinct[distinct_key] = "Y";
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
	self.writer.LineStart("printf \"")
	for idx, _ := range output.VarList {
		self.writer.O("%s")
		self.writer.O(self.cg.OutputSeparator)
	}
	self.writer.O("\",")

	x := self.outputLocalList(output)
	self.writer.O(strings.Join(x, ","))
	self.writer.LineDone(";")
}

func (self *outputCodeGenNormal) genSortOutput(
	output *plan.Output,
) error {
	return generateSortOutput(
		self.cg,
		self.writer,
		output.SortList,
	)
}

func (self *outputCodeGenNormal) genSortOutput(
	output *plan.Output,
) error {
	self.writer.Line("output_count++;")
	if err := self.genVarOutput(output); err != nil {
		return err
	}
	if err := self.genSortOutput(output); err != nil {
		return err
	}
	self.writer.Line("print \"\"")
	return nil
}

func (self *outputCodeGenNormal) genNext(output *plan.Output) error {
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

func (self *outputCodeGenNormal) genFlush() error {
	return nil
}

func (self *outputCodeGenNormal) genDone() error {
	return nil
}
