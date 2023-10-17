package cg

import (
	"fmt"
	"github.com/dianpeng/sql2awk/plan"
)

// Aggregation Generation.
//
// the aggregation generation is kind of simple, for each variable that requires
// generation, we just record the value and then perform aggregation operartion
// accordingly later on

type aggCodeGen struct {
	cg     *queryCodeGen
	writer *awkWriter
}

func (self *aggCodeGen) setWriter(w *awkWriter) {
	self.writer = w
}

func (self *aggCodeGen) genCalc(
	l []plan.AggVar,
) error {
	for idx, expr := range l {
		str := self.cg.genExpr(expr.Target)
		self.writer.Assign(
			self.writer.LocalN("agg_tmp", idx),
			str,
			nil,
		)
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
		awkWriterCtx{
			"var": self.writer.GlobalN("agg_val", idx),
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
		awkWriterCtx{
			"var": self.writer.GlobalN("agg_val", idx),
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
		awkWriterCtx{
			"var": self.writer.GlobalN("agg_val", idx),
			"tmp": self.writer.LocalN("agg_tmp", idx),
		},
	)
}

func (self *aggCodeGen) genAggAvg(
	idx int,
) {
	self.genaggsum(idx)
}

func (self *aggCodeGen) genAggSum(
	idx int,
) {
	self.genaggsum(idx)
}

func (self *aggCodeGen) genAggCount(
	idx int,
) {
	self.writer.Assign(
		self.writer.GlobalN("agg_val", idx),
		self.writer.Global("agg_count"),
		nil,
	)
}

func (self *aggCodeGen) genAggPercentile(
	idx int,
) {
	self.writer.Line(
		`%[agg_val][$[g, agg_count]""] = kv_make(order_key(%[agg_tmp]), %[agg_tmp]);`,
		awkWriterCtx{
			"agg_val": self.writer.GlobalNArray("agg_val", idx),
			"agg_tmp": self.writer.LocalN("agg_tmp", idx),
		},
	)
}

func (self *aggCodeGen) genAggHistogram(
	idx int,
) {
	self.writer.Line(
		`%[agg_val][$[g, agg_count]""] = %[agg_tmp];`,
		awkWriterCtx{
			"agg_val": self.writer.GlobalNArray("agg_val", idx),
			"agg_tmp": self.writer.LocalN("agg_tmp", idx),
		},
	)
}

func (self *aggCodeGen) genAggOutput(l []plan.AggVar) {
	for idx, v := range l {
		switch v.AggType {
		default:
			break

		case plan.AggMin, plan.AggMax, plan.AggCount, plan.AggSum:
			self.writer.Assign(
				self.writer.ArrIdxN("agg", idx),
				self.writer.GlobalN("agg_val", idx),
				nil,
			)
			break

		case plan.AggAvg:
			self.writer.Assign(
				self.writer.ArrIdxN("agg", idx),
				"(%[val]+0.0)/$[g, agg_count]",
				awkWriterCtx{
					"val": self.writer.GlobalN("agg_val", idx),
				},
			)
			break

		case plan.AggPercentile:
			// percentile has one extra argument to indicate the percentile number
			// which is a constant number
			nPercent := 50
			if v, has := v.ParamInt(1); has && v >= 0 && v <= 100 {
				nPercent = int(v)
			}

			// okay, now calls a builtin function to perform percentile calculation
			self.writer.Assign(
				self.writer.ArrIdxN("agg", idx),
				"kv_getv(agg_percentile(%[input], %[n]))",
				awkWriterCtx{
					"input": self.writer.GlobalNArray("agg_val", idx),
					"n":     fmt.Sprintf("%d", nPercent),
				},
			)
			break

		case plan.AggHistogram:
			// percentile has one extra argument to indicate the percentile number
			// which is a constant number
			min := "0"
			max := "100"
			bin := "5"
			sep := ";"

			if v, has := v.ParamNum(1); has {
				min = v
			}
			if v, has := v.ParamNum(2); has {
				max = v
			}
			if v, has := v.ParamNum(3); has {
				bin = v
			}
			if v, has := v.ParamNum(4); has {
				sep = v
			}

			// okay, now calls a builtin function to perform percentile calculation
			self.writer.Assign(
				self.writer.ArrIdxN("agg", idx),
				"agg_histogram(%[input], 1, $[g, agg_count], %[min], %[max], %[bin], \"%[sep]\")",
				awkWriterCtx{
					"input": self.writer.GlobalNArray("agg_val", idx),
					"min":   min,
					"max":   max,
					"bin":   bin,
					"sep":   sep,
				},
			)
			break
		}
	}
}

func (self *aggCodeGen) genAggCleanup(l []plan.AggVar) {
	self.writer.Assign(
		"$[g, agg_count]",
		"0",
		nil,
	)
	for idx, _ := range l {
		if !self.writer.HasGlobalNArray("agg_val", idx) {
			self.writer.Assign(
				self.writer.GlobalN("agg_val", idx),
				"\"\"", // use empty string
				nil,
			)
		} else {
			self.writer.Line(
				"clear_array(%[gvar])",
				awkWriterCtx{
					"gvar": self.writer.GlobalN("agg_val", idx),
				},
			)
		}
	}
}

func (self *aggCodeGen) genNext() error {
	self.writer.Line(
		"$[g, agg_count]++;",
		nil,
	)
	// save all the rid to be used during agg_flush.
	for i := 0; i < self.cg.tsSize(); i++ {
		self.writer.Assign(
			self.writer.GlobalN("agg_rid", i),
			self.writer.rid(i),
			nil,
		)
	}

	agg := self.cg.query.Agg
	if agg == nil {
		return nil
	}

	l := agg.VarList

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

		case plan.AggPercentile:
			self.genAggPercentile(idx)
			break

		case plan.AggHistogram:
			self.genAggHistogram(idx)
			break

		default:
			break
		}
	}
	return nil
}

func (self *aggCodeGen) genFlush() error {
	agg := self.cg.query.Agg

	if agg != nil {
		l := agg.VarList
		self.genAggOutput(l)
		self.genAggCleanup(l)
	}

	self.writer.Call(
		"having_next",
		self.writer.GlobalParamList("agg_rid", self.cg.tsSize()),
	)
	return nil
}

func (self *aggCodeGen) genDone() error {
	self.writer.CallPipelineFlush(
		"having",
	)
	self.writer.CallPipelineDone(
		"having",
	)
	return nil
}
