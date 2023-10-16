package cg

// Having phase generation
type havingCodeGen struct {
	cg     *queryCodeGen
	writer *awkWriter
}

func (self *havingCodeGen) setWriter(w *awkWriter) {
	self.writer = w
}

// having is easy, just add a filter that's it
func (self *havingCodeGen) genNext() error {
	having := self.cg.query.Having
	if having != nil {
		having := self.cg.query.Having
		fexpr := self.cg.genExpr(having.Filter)
		self.writer.Chunk(
			"if (!(%[filter])) return;",
			awkWriterCtx{
				"filter": fexpr,
			},
		)
	}

	self.writer.CallPipelineNext(
		"output",
	)
	return nil
}

func (self *havingCodeGen) genFlush() error {
	self.writer.CallPipelineFlush(
		"output",
	)
	return nil
}

func (self *havingCodeGen) genDone() error {
	self.writer.CallPipelineDone(
		"output",
	)
	return nil
}
