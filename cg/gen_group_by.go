package cg

import (
	"fmt"
	"strings"
)

// GroupBy code generation
// ----------------------------------------------------------------------------
// The group by phase is materialized as following
//
// we setup a *group_by* table which contains reverse index, from group by
// tuple to its corresponding table row index tuple. The table row index tuple
// is been store as "i0, i1, i2, ..., in", which can be easily splitted via
// split function from awk. We have dedicated logic to walk through the group_by
// table and then yield the tuple in group by order correctly
//
// the code splitted into 2 parts, notes, if group by is applicable. The first
// part is group_by_next function, which contains hash table setup part.

type groupByCodeGen struct {
	cg     *queryCodeGen
	writer *awkWriter
	tsSize int // table scan size
}

func (self *groupByCodeGen) setWriter(w *awkWriter) {
	self.writer = w
}

func (self *groupByCodeGen) perItemGroupBy() bool {
	q := self.cg.query
	return q.GroupBy == nil && q.Agg == nil
}

func (self *groupByCodeGen) genNext() error {
	groupBy := self.cg.query.GroupBy
	if groupBy != nil {
		for idx, fexpr := range groupBy.VarList {
			str := self.cg.genExpr(fexpr)
			self.writer.Assign(
				"gb_expr%[idx]",
				str,
				map[string]interface{}{
					"idx": idx,
				},
			)
		}

		// perform concatenation of the expression generated, and convert all the
		// expression to *string*. Otherwise we don't know the type
		buf := []string{}
		l := len(groupBy.VarList)
		for i := 0; i < l; i++ {
			buf = append(buf, fmt.Sprintf("(gb_expr%d\"\")", i))
		}

		self.writer.Assign(
			"gb_key",
			strings.Join(buf, " "),
			nil,
		)

		// table metadata, ie current count
		{
			self.writer.Chunk(
				`
if (group_by[gb_key] == "") {
  group_by[gb_key]=1;
  idx = 0;
} else {
  idx = group_by[gb_key];
  group_by[gb_key]++;
}
  `,
				nil,
			)
		}

		// table value
		{
			self.writer.Line(
				"group_by_index[sprintf(\"%s:%d\", gb_key, idx)] = %[value];",
				awkWriterCtx{
					"value": self.writer.ridCommaList(self.cg.tsSize()),
				},
			)
		}
	} else {
		self.writer.CallPipelineNext(
			"agg",
		)
		if self.perItemGroupBy() {
			self.writer.CallPipelineFlush(
				"agg",
			)
		}
	}

	return nil
}

// ----------------------------------------------------------------------------
// the flush part of the group by is essentially, just walk through group_by
// table and use the value to decide how to index back into group_by_index table
func (self *groupByCodeGen) genFlush() error {
	groupBy := self.cg.query.GroupBy
	if groupBy != nil {

		self.writer.Chunk(
			`
for (gb_key_tt in group_by) {
  tt = group_by[gb_key_tt];               # must be a number
  for (i = 0; i < tt; i++) {              # inner loop
    key = sprintf("%s:%d", gb_key_tt, i); # get the key
    val = group_by_index[key];            # get the ',' delimited column size
    split(val, sep, ",");                 # split the ',' into list of column index
  `,
			nil,
		)

		// agg_next's argument is access of *sep* array
		arg := []string{}
		for i := 0; i < self.tsSize; i++ {
			// separater's index starts with 1, funny
			arg = append(arg, fmt.Sprintf("sep[%d]", i+1))
		}

		self.writer.Call(
			"agg_next",
			arg,
		)

		self.writer.Chunk(
			`
  }
  agg_flush();
}
  `,
			nil,
		)
	} else if !self.perItemGroupBy() {
		self.writer.CallPipelineFlush(
			"agg",
		)
	}

	return nil
}

func (self *groupByCodeGen) genDone() error {
	self.writer.CallPipelineDone(
		"agg",
	)
	return nil
}
