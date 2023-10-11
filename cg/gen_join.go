package cg

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
	cg      *CodeGen
	maxJoin int
}

func (self *joinCodeGen) genOuterJoin(
	idx int,
	j plan.Join,
	ref *tableScanGenRef,
	writer *awkWriter,
) {
	writer.For(
		"i%[idx]  = 0; i%[idx] < %[size]; i%[idx]++",
		map[string]string{
      "idx": idx,
			"size": ref.Size,
		},
	)

	writer.EndFor()
}

func (self *joinCodeGen) genInnerJoin(
	idx int,
	j plan.Join,
	ref *tableScanGenRef,
	writer *awkWriter,
) {
	writer.For(
		"#i[Idx] = 0; #i[Idx] < %[Size]; #i[Idx]++",
		map[string]string{
			"Size": ref.Size,
		},
	)

	// filter of join, if any, otherwise just do nothing at all
	if filter := j.Filter(); filter != nil {
		if fStr, err := self.cg.genExpr(filter); err != nil {
			return err
		}
		writer.Line(
			"if (!%[Filter]) continue;",
			map[string]string{
				"Filter": fStr,
			},
		)
	}

	// invoke nest phase, which is *group_by*
	writer.Call(
		"group_by_next",
		self.loopInductionVariableList(idx+1),
	)

	// end the inner most for
	writer.EndFor()
}

func (self *joinCodeGen) loopInductionVariableList() []string {
	out := []string{}
	for i := 0; i < self.maxJoin; i++ {
		out = append(out, fmt.Sprintf("i%d", i))
	}
	return out
}
