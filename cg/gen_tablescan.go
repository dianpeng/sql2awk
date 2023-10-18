package cg

import (
	"github.com/dianpeng/sql2awk/plan"
	"strings"
	"text/template"
)

type tableScanGenRef struct {
	Size  string
	Field string
	Table string
}

type tableScanGen struct {
	cg  *queryCodeGen
	Ref []tableScanGenRef
}

const tableScanTemplate = `
  if (FILENAME == "{{.Filename}}") {
    {{if ne .Filter ""}}
    if (!({{.Filter}})) next;
    {{end}}
    row_idx = {{.VarTableLength}};
    {{.VarTableLength}}++;
    field_count_tt = NF;
    {{if not .FullColumn }}
    field_count_tt = {{.MaxColumn}} < NF ? {{.MaxColumn}} : NF;
    {{end}}
    if ({{.VarTableField}} < field_count_tt) {
      {{.VarTableField}} = field_count_tt;
    }
    {{.VarTableField}} = field_count_tt;
    for (i = 1; i <= field_count_tt; i++) {
      {{.VarTable}}[row_idx, i] = $i;
    }
    next;
  }

`

func newTemplate(
	xx string,
) (*template.Template, error) {
	return template.New("[template]").Parse(xx)
}

func (self *tableScanGen) genOne(
	ts *plan.TableScan,
) string {
	filter := ""
	if ts.Filter != nil {
		filter = self.cg.genExpr(ts.Filter)
	}

	t, err := newTemplate(
		tableScanTemplate,
	)
	if err != nil {
		panic("codegen(TableScan): invalid template?")
	}

	out := &strings.Builder{}
	x := tableScanGenRef{
		Table: self.cg.varTable(ts.Table.Index),
		Field: self.cg.varTableField(ts.Table.Index),
		Size:  self.cg.varTableSize(ts.Table.Index),
	}
	self.Ref = append(self.Ref, x)

	if err := t.Execute(out, map[string]interface{}{
		"Filename":       ts.Table.Path,
		"MaxColumn":      ts.Table.MaxColumn,
		"FullColumn":     ts.Table.FullColumn,
		"Filter":         filter,
		"VarTable":       x.Table,
		"VarTableLength": x.Size,
		"VarTableField":  x.Field,
	}); err != nil {
		panic(err.Error())
	}
	return out.String()
}

func (self *tableScanGen) gen(
	p *plan.Plan,
) string {
	buf := &strings.Builder{}
	for _, ts := range p.TableScan {
		data := self.genOne(ts)
		buf.WriteString(data)
	}
	return buf.String()
}
