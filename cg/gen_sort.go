package cg

import (
	"fmt"
)

// ----------------------------------------------------------------------------
// Sorting phase. This phase requires the usage of *asort* function which is
// extension from gawk, mawk, but not the original awk. This function can be
// somehow faked using other method, like linux *sort* command line, but it
// is kind of complicated. For now, we just generate code utilizing *asort*
// which is wildly existed.
// ----------------------------------------------------------------------------
// The baisc idea for generation *sort* is by following
// 1) setup a global array *sort_index*
//   1.1) evaluate sort key tuple, set it as sort_key
//   1.2) populate *value* as current rid list, key as *sort_key*, in sort_index
// 2) Once the genFlush() is called, it starts the sorting
//   2.1) call *asorti* for sort_index
//   2.2) iterate the returned sortted array
//   2.3) use key to access the *sort_index* array one by one and call next

type sortCodeGen struct {
	cg     *queryCodeGen
	writer *awkWriter
}

func (self *sortCodeGen) setWriter(w *awkWriter) {
	self.writer = w
}

func (self *sortCodeGen) genNext() error {
	if sort := self.cg.query.Sort; sort != nil {
		if self.cg.useGoAWK {
			return fmt.Errorf(
				"GoAWK cannot correctly generate *sort* and does not have builtin " +
					"function to sort, additionally, it also does not have correct " +
					"meachnism to register a go function to support array output, currently " +
					"sort is not supported by GoAWK",
			)
		}
		self.writer.Assign(
			"$[l, sort_key]",
			"\"\"",
			nil,
		)

		for _, v := range sort.VarList {
			expr := self.cg.genExpr(v)
			self.writer.Chunk(
				`
$[l, expr] = %[expr];
$[l, sort_key] = sprintf("%s%s", $[l, sort_key], order_key($[l, expr]));
`,
				awkWriterCtx{
					"expr": expr,
				},
			)
		}

		// the following algorithm is kind of complicated, the whole point is to
		// workaround the limitation of awk itself, which is not very capable in
		// our case
		self.writer.Chunk(
			`
$[l, sort_key_idx]=0;
if (length($[ga, sort_index][$[l, sort_key]]) == 0) {
  $[ga, sort_index][$[l, sort_key]] = 1;
  $[l, sort_key_idx]=0;
} else {
  $[l, sort_key_idx] = $[ga, sort_index][$[l, sort_key]];
  $[ga, sort_index][$[l, sort_key]]++;
}
$[l, sort_value_key] = sprintf("%s%s", $[l, sort_key], $[l, sort_key_idx]);
$[ga, sort_value][$[l, sort_value_key]] = %[rid_list];
`,
			awkWriterCtx{
				"rid_list": self.writer.ridCommaList(self.cg.tsSize()),
			},
		)
	} else {
		self.writer.CallPipelineNext(
			"output",
		)
	}
	return nil
}

func (self *sortCodeGen) genFlush() error {
	if sort := self.cg.query.Sort; sort != nil {
		if self.cg.useGoAWK {
			return fmt.Errorf(
				"GoAWK cannot correctly generate *sort* and does not have builtin " +
					"function to sort, additionally, it also does not have correct " +
					"meachnism to register a go function to support array output, currently " +
					"sort is not supported by GoAWK",
			)
		}

		self.writer.Line(
			`$[l, sort_output_length] = asorti($[ga, sort_index], $[ga, sort_output]);`,
			nil,
		)
		self.writer.Chunk(
			`
for ($[l, sort_idx] = 1; $[l, sort_idx] <= $[l, sort_output_length]; $[l, sort_idx]++) {
  $[l, sort_idx_key] = $[ga, sort_output][$[l, sort_idx]];
  $[l, sort_rid_list_size] = $[ga, sort_index][$[l, sort_idx_key]]+0;
  for ($[l, rid_list_idx] = 0; $[l, rid_list_idx] < $[l, sort_rid_list_size]; $[l, rid_list_idx]++) {
    $[l, rid_list_key] = sprintf("%s%s", $[l, sort_idx_key], $[l, rid_list_idx]);
    $[l, sort_rid_list] = $[ga, sort_value][$[l, rid_list_key]];
    split($[l, sort_rid_list], $[l, rid_list], ",");
    output_next(%[rid_args]);
  }
}
output_flush();
`,
			awkWriterCtx{
				"rid_args": self.writer.SpreadArr("$[l, rid_list]", 1, 1+self.cg.tsSize(), nil),
			},
		)
	} else {
		self.writer.CallPipelineFlush("output")
	}
	return nil
}

func (self *sortCodeGen) genDone() error {
	self.writer.CallPipelineDone("output")
	return nil
}
