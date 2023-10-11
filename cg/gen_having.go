package cg

// Having phase generation
type havingCodeGen struct {
	cg     *CodeGen
	writer *awkWriter
}

// having is easy, just add a filter that's it
func (self *havingCodeGen) genNext(having *plan.Having) error {
	if fexpr, err := self.cg.genExpr(having.Filter); err != nil {
		return err
	} else {
		self.writer.Chunk(
			"if (!(%[filter])) return;",
			map[string]interface{}{
				"filter": fexpr,
			},
		)
		self.writer.CallPipelineNext(
			"output",
		)
		return nil
	}
}

func (self *havingCodeGen) genFlush(having *plan.Having) error {
	self.writer.CallPipelineNext(
		"output",
	)
	return nil
}

func (self *havingCodeGen) genDone(having *plan.Having) error {
	self.writer.CallPipelineDone(
		"output",
	)
	return nil
}
