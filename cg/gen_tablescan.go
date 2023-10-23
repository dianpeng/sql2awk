package cg

import (
	"github.com/dianpeng/sql2awk/plan"
)

type tableScanGenRef struct {
	Size  string
	Field string
	Table string
}

type tableScanGen struct {
	cg     *queryCodeGen
	writer *awkWriter
	Ref    []tableScanGenRef
}

func (self *tableScanGen) genOneTab(
	ts *plan.TableScan,
) error {
	filter := ""
	if ts.Filter != nil {
		filter = self.cg.genExpr(ts.Filter)
	}

	fs := ts.Table.Params.AsStr(0, "")
	start := ts.Table.Params.AsInt(1, -1)
	end := ts.Table.Params.AsInt(2, -1)
	table := ts.Table

	x := tableScanGenRef{
		Table: self.cg.varTable(table.Index),
		Field: self.cg.varTableField(table.Index),
		Size:  self.cg.varTableSize(table.Index),
	}

	self.writer.If(
		`FILENAME=="%[filename]"`,
		awkWriterCtx{
			"filename": ts.Table.Path,
		},
	)

	// scanning filter code
	{
		if fs != "" {
			self.writer.Chunk(
				`
    # workaround the issue with FS setting dynamically with awk. Additionally
    # workaround the split with regexp issue, notes the regexp is static
    if (FNR <= 1) {
      # always the first line have the issue, we will *NOT* use NF, but
      # use manual split hera, notes the FS will be treated as static regexp
      # whose type will not touch the split function implementation bug, which
      # not always use regexp search :(
      __workaround_sep_n = split($0, __workaround_sep, /%[fs]/);
      NF = __workaround_sep_n;
      for (__workaround_i = 1; __workaround_i <= NF; __workaround_i++) {
        $__workaround_i = __workaround_sep[__workaround_i];
      }
    }
    FS="%[fs]";
  `,
				awkWriterCtx{
					"fs": fs,
				},
			)
		}

		if start > 0 {
			self.writer.Line(
				`if (FNR <= %[start]) next`,
				awkWriterCtx{
					"start": start,
				},
			)
		}

		if end > 0 {
			self.writer.Line(
				`if (FNR > %[end]) nextfile;`,
				awkWriterCtx{
					"end": end,
				},
			)
		}
	}

	// rewrite
	if rewrite := table.Rewrite; rewrite != nil {
		for _, stmt := range rewrite.Stmt {
			self.writer.If(
				"%[cond]",
				awkWriterCtx{
					"cond": self.cg.genExpr(stmt.Cond),
				},
			)

			for _, set := range stmt.Set {
				self.writer.Line(
					"$%[idx] = %[value];",
					awkWriterCtx{
						"idx":   set.Column,
						"value": self.cg.genExpr(set.Value),
					},
				)
			}
			self.writer.IfEnd()
		}
	}

	// table gen
	{
		if filter != "" {
			self.writer.Line(
				`if (!(%[filter])) next;`,
				awkWriterCtx{
					"filter": filter,
				},
			)
		}
		self.writer.Chunk(
			`
row_idx = %[table_size];
%[table_size]++;
`,
			awkWriterCtx{
				"table":      x.Table,
				"table_size": x.Size,
			},
		)

		if !table.FullColumn {
			self.writer.Line(
				`field_count_tt = %[max_column] < NF ? %[max_column] : NF;`,
				awkWriterCtx{
					"max_column": table.MaxColumn,
				},
			)
		} else {
			self.writer.Line("field_count_tt = NF;", nil)
		}

		self.writer.Chunk(
			`
if (%[table_field] < field_count_tt) {
  %[table_field] = field_count_tt;
}
for (i = 1; i <= field_count_tt; i++) {
  %[table][row_idx, i] = $i;
}
next;
`,
			awkWriterCtx{
				"table":       x.Table,
				"table_size":  x.Size,
				"table_field": x.Field,
			},
		)
	}

	self.Ref = append(self.Ref, x)
	self.writer.IfEnd()
	return nil
}

func (self *tableScanGen) gen(
	p *plan.Plan,
) error {
	for _, ts := range p.TableScan {
		switch ts.Table.Type {
		case "tab", "Tab":
			if err := self.genOneTab(ts); err != nil {
				return err
			}
			break

		default:
			panic("unknown table type")
			break
		}
	}
	return nil
}
