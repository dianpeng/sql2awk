package plan

import (
	"fmt"
	"github.com/dianpeng/sql2awk/sql"
	"strings"
)

// Printing the plan out, for testing, debugging, visualization purpose etc ...

func (self *Plan) Print() string {
	buf := &strings.Builder{}
	self.printTableList(buf)
	self.printTableScanList(buf)
	self.printJoin(buf)
	self.printGroupBy(buf)
	self.printAgg(buf)
	self.printHaving(buf)
	self.printOutput(buf)
	self.printSort(buf)
	return buf.String()
}

func (self *Plan) printTableDescriptor(
	ts *TableDescriptor,
	buf *strings.Builder,
) {
	buf.WriteString("##> Table Descriptor\n")
	buf.WriteString(fmt.Sprintf("Index: %d\n", ts.Index))
	buf.WriteString(fmt.Sprintf("Path: %s\n", ts.Path))
	buf.WriteString(fmt.Sprintf("Type: %s\n", ts.Type))
	buf.WriteString(fmt.Sprintf("Alias: %s\n", ts.Alias))
	buf.WriteString(fmt.Sprintf("Options: %s\n", ts.Options.Print()))
	buf.WriteString(fmt.Sprintf("Symbol: %s\n", ts.Symbol))
	buf.WriteString(fmt.Sprintf("MaxColumn: %d\n", ts.MaxColumn))
	buf.WriteString(fmt.Sprintf("FullColumn: %v\n", ts.FullColumn))
}

func (self *Plan) printTableList(
	buf *strings.Builder,
) {
	for _, ts := range self.tableList {
		self.printTableDescriptor(ts, buf)
	}
}

func (self *Plan) printTableScan(
	ts *TableScan,
	buf *strings.Builder,
) {
	buf.WriteString("##> TableScan\n")
	buf.WriteString(fmt.Sprintf("Table: %d\n", ts.Table.Index))
	buf.WriteString(fmt.Sprintf("Filter: %s\n", sql.PrintExpr(ts.Filter)))
}

func (self *Plan) printTableScanList(
	buf *strings.Builder,
) {
	for _, ts := range self.TableScan {
		self.printTableScan(ts, buf)
	}
}

func (self *Plan) printJoin(
	buf *strings.Builder,
) {
	j := self.Join
	buf.WriteString(j.Dump())
}

func (self *Plan) printGroupBy(
	buf *strings.Builder,
) {
	groupBy := self.GroupBy
	buf.WriteString("##> GroupBy\n")
	if groupBy == nil {
		buf.WriteString("--\n")
	} else {
		for idx, expr := range groupBy.VarList {
			buf.WriteString(fmt.Sprintf("Var[%d]: %s\n", idx, sql.PrintExpr(expr)))
		}
	}
}

func (self *Plan) printAgg(
	buf *strings.Builder,
) {
	agg := self.Agg
	buf.WriteString("##> Agg\n")
	if agg == nil {
		buf.WriteString("--\n")
	} else {
		for idx, avar := range agg.VarList {
			buf.WriteString(
				fmt.Sprintf(
					"Var[%d]: %s(%s)\n",
					idx,
					avar.AggName(),
					sql.PrintExpr(avar.Value),
				),
			)
		}
	}
}

func (self *Plan) printHaving(
	buf *strings.Builder,
) {
	having := self.Having
	buf.WriteString("##> Having\n")
	if having == nil {
		buf.WriteString("--\n")
	} else {
		buf.WriteString(fmt.Sprintf("Filter: %s\n", sql.PrintExpr(having.Filter)))
	}
}

func (self *Plan) printOutput(
	buf *strings.Builder,
) {
	output := self.Output
	buf.WriteString("##> Output\n")
	buf.WriteString(fmt.Sprintf("Limit: %d\n", output.Limit))
	buf.WriteString(fmt.Sprintf("Distinct: %v\n", output.Distinct))

	for idx, ovar := range output.VarList {
		switch ovar.Type {
		case OutputVarWildcard:
			buf.WriteString(
				fmt.Sprintf(
					"Var[%d]: %s\n",
					idx,
					fmt.Sprintf("tbl[%d].*", ovar.Table.Index),
				),
			)
			break

		case OutputVarRowMatch:
			buf.WriteString(
				fmt.Sprintf(
					"Var[%d]: %s\n",
					idx,
					fmt.Sprintf(".ROWS(%s)", ovar.Pattern),
				),
			)
			break

		case OutputVarColMatch:
			buf.WriteString(
				fmt.Sprintf(
					"Var[%d]: %s\n",
					idx,
					fmt.Sprintf(".COLUMNS(%s)", ovar.Pattern),
				),
			)
			break

		default:
			buf.WriteString(fmt.Sprintf("Var[%d]: %s\n", idx, sql.PrintExpr(ovar.Value)))
			break
		}
	}

}

func (self *Plan) printSort(
	buf *strings.Builder,
) {
	sort := self.Sort
	buf.WriteString("##> OrderBy\n")
	if sort == nil {
		buf.WriteString("--\n")
	} else {
		if sort.Asc {
			buf.WriteString("Order: asc\n")
		} else {
			buf.WriteString("Order: desc\n")
		}
		for idx, expr := range sort.VarList {
			buf.WriteString(fmt.Sprintf("Sort[%d]: %s\n", idx, sql.PrintExpr(expr)))
		}
	}
}
