package cg

import (
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
				self.writer.LocalN("gb_expr", idx),
				str,
				nil,
			)
		}

		// perform concatenation of the expression generated, and convert all the
		// expression to *string*. Otherwise we don't know the type
		buf := []string{}
		l := len(groupBy.VarList)
		for i := 0; i < l; i++ {
			buf = append(buf, self.writer.LocalN("gb_expr", i))
		}

		self.writer.Assign(
			self.writer.Local("gb_key"),
			strings.Join(buf, " "),
			nil,
		)

		// table metadata, ie current count
		{
			self.writer.Chunk(
				`
if ($[ga, group_by][$[l, gb_key]] == "") {
  $[ga, group_by][$[l, gb_key]]=1;
  $[l, index] = 0;
} else {
  $[l, index] = $[ga, group_by][$[l, gb_key]];
  $[ga, group_by][$[l, gb_key]]++;
}
  `,
				nil,
			)
		}

		// table value
		{
			self.writer.Line(
				"$[ga, group_by_index][sprintf(\"%s:%d\", $[l, gb_key], $[l, index])] = %[value];",
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
for ($[l, gb_key_tt] in $[ga, group_by]) {
  $[l, tt] = $[ga, group_by][$[l, gb_key_tt]];              # must be a number
  for ($[l, i] = 0; $[l, i] < $[l, tt]; $[l, i]++) {        # inner loop
    $[l, key] = sprintf("%s:%d", $[l, gb_key_tt], $[l, i]); # get the key
    $[l, val] = $[ga, group_by_index][$[l, key]];           # get the rid list
    split($[l, val], $[l, sep], ",");                       # split the ','
  `,
			nil,
		)

		// agg_next's argument is access of *sep* array
		arg := []string{}
		for i := 0; i < self.tsSize; i++ {
			// separater's index starts with 1, funny
			arg = append(arg, self.writer.Fmt(
				"$[l, sep][%[idx]]",
				awkWriterCtx{
					"idx": i + 1,
				},
			))
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
