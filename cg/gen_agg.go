package cg

// Aggregation Generation.
//
// the aggregation generation is kind of simple, for each variable that requires
// generation, we just record the value and then perform aggregation operartion
// accordingly later on

type aggCodeGen struct {
	cg *CodeGen
}

func (self *aggCodeGen) genCalc(
	l []plan.AggVar,
) error {
	for idx, expr := range l {
		if str, err := self.cg.genExpr(expr); err != nil {
			return err
		} else {
			self.writer.Assign(
				self.writer.LocalN("agg_tmp", idx),
				str,
			)
		}
	}
	return nil
}

func (self *aggCodeGen) genAggMin(
	idx int,
) {
	self.writer.Chunk(
		`
  if (%[var] == "") {
    %[var] = %[tmp];
  } else if (%[var] > %[tmp]) {
    %[var] = %[tmp];
  }
`,
		map[string]interface{}{
			"var": self.writer.Var("agg_val", idx),
			"tmp": self.writer.LocalN("agg_tmp", idx),
		},
	)
}

func (self *aggCodeGen) genAggMax(
	idx int,
) {
	self.writer.Chunk(
		`
  if (%[var] == "") {
    %[var] = %[tmp];
  } else if (%[var] < %[tmp]) {
    %[var] = %[tmp];
  }
`,
		map[string]interface{}{
			"var": self.writer.Var("agg_val", idx),
			"tmp": self.writer.LocalN("agg_tmp", idx),
		},
	)
}

func (self *aggCodeGen) genaggsum(
	idx int,
) {
	self.writer.Chunk(
		`
  if (%[var] == "") {
    %[var] = (%[tmp]+0.0);
  } else {
    %[var] += (%[tmp]+0.0);
  }
`,
		map[string]interface{}{
			"var": self.writer.Var("agg_val", idx),
			"tmp": self.writer.LocalN("agg_tmp", idx),
		},
	)
}

func (self *aggCodeGen) genAggAvg(
	idx int,
) {
	return self.genaggsum(idx)
}

func (self *aggCodeGen) genAggSum(
	idx int,
) {
	return self.genaggsum(idx)
}

func (self *aggCodeGen) genAggCount(
	idx int,
) {
	self.writer.Assign(
		self.writer.LocalN("agg_val", idx),
		"agg_count", // agg_count is always been updated internally
	)
}

func (self *aggCodeGen) genAggOutput(l []plan.AggVar) {
	for idx, v := range l {
		switch v.AggType {
		case plan.AggMin, plan.AggMax, plan.AggCount, plan.AggSum:
			self.writer.Assign(
				cgArrIdx("agg", idx),
				self.writer.LocalN("agg_val", idx),
			)
			break

		case plan.AggAvg:
			self.writer.Assign(
				cgArrIdx("agg", idx),
				fmt.Sprintf("(%s+0.0)/agg_count", self.writer.LocalN("agg_val", idx)),
			)
			break
		}
	}
}

func (self *aggCodeGen) genNext(l []plan.AggVar) error {
	if err := self.genCalc(l); err != nil {
		return err
	}
	for idx, x := range l {
		switch x.AggType {
		case plan.AggMin:
			self.genAggMin(idx)
			break

		case plan.AggMax:
			self.genAggMax(idx)
			break

		case plan.AggSum:
			self.genAggSum(idx)
			break

		case plan.AggAvg:
			self.genAggAvg(idx)
			break

		case plan.AggCount:
			self.genAggCount(idx)
			break

		default:
			break
		}
	}
}

func (self *aggCodeGen) genFlush(l []plan.AggVar) error {
	self.genAggOutput(l)
	self.writer.Assign(
		"agg_count",
		"0",
	)
	self.writer.CallPipelineNext(
		"having",
	)
	return nil
}

func (self *aggCodeGen) genDone(l []plan.AggVar) error {
	self.writer.CallPipelineFlush(
		"having",
	)
	self.writer.CallPipelineDone(
		"having",
	)
	return nil
}
