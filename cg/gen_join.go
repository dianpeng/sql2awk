package cg

import (
	"fmt"
	"github.com/dianpeng/sql2awk/plan"
)

// ----------------------------------------------------------------------------
// Join function body's code generation. The skeleton of the join body is
// as following:
//
// for (i0 = 0; i0 < tblsize0; i0++) {
//   for (i1 = 0; i1 < tblsize1; i1++) {
//     ...
//     for (in = 0; in < tabsizen; in++) {
//        if (!(filter)) continue; // skip this line since join failed
//        group_by_next(i0, i1, i2, ..., in); // inovke the next phase
//     }
//   }
// }
// group_by_flush(); // flush the data
// group_by_done();  // join is done

type joinCodeGen struct {
	cg *queryCodeGen
}

func (self *joinCodeGen) genOuterJoin(
	idx int,
	j plan.Join,
	ref *tableScanGenRef,
	writer *awkWriter,
) {
	writer.For(
		"rid_%[idx]  = 0; rid_%[idx] < %[size]; rid_%[idx]++",
		awkWriterCtx{
			"idx":  idx,
			"size": ref.Size,
		},
	)

	if (idx + 1) < self.cg.tsSize()-1 {
		self.genOuterJoin(
			idx+1,
			j,
			&self.cg.tsRef[idx+1],
			writer,
		)
	} else {
		self.genInnerJoin(
			idx+1,
			j,
			&self.cg.tsRef[idx+1],
			writer,
		)
	}

	writer.ForEnd()
}

func (self *joinCodeGen) genInnerJoin(
	idx int,
	j plan.Join,
	ref *tableScanGenRef,
	writer *awkWriter,
) {
	writer.DefineGlobal("join_size")

	writer.For(
		"rid_%[idx] = 0; rid_%[idx] < %[size]; rid_%[idx]++",
		awkWriterCtx{
			"idx":  idx,
			"size": ref.Size,
		},
	)

	writer.Line(
		"$[g, join_size]++;",
		nil,
	)

	// filter of join, if any, otherwise just do nothing at all
	if filter := j.JoinFilter(); filter != nil {
		writer.Line(
			"if (!%[filter]) continue;",

			awkWriterCtx{
				"filter": self.cg.genExpr(filter),
			},
		)
	}

	// invoke nest phase, which is *group_by*
	writer.Call(
		"group_by_next",
		self.loopInductionVariableList(),
	)

	// end the inner most for
	writer.ForEnd()
}

func (self *joinCodeGen) genJoin(writer *awkWriter) {
	nIdx := 0
	j := self.cg.query.Join
	ref := &self.cg.tsRef[nIdx]
	tsSize := self.cg.tsSize()

	if nIdx < tsSize-1 {
		self.genOuterJoin(
			nIdx,
			j,
			ref,
			writer,
		)
	} else {
		self.genInnerJoin(
			nIdx,
			j,
			ref,
			writer,
		)
	}

	writer.Chunk(
		`
if ($[g, join_size] > 0) {
  @[pipeline_flush, %(group_by)];
  @[pipeline_done, %(group_by)];
}
  `,
		nil,
	)
}

func (self *joinCodeGen) loopInductionVariableList() []string {
	out := []string{}
	maxJoin := self.cg.tsSize()
	for i := 0; i < maxJoin; i++ {
		out = append(out, fmt.Sprintf("rid_%d", i))
	}
	return out
}
