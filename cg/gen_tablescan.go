package cg

import (
	"github.com/dianpeng/sql2awk/plan"
	"strings"
)

type tableScanGenRef struct {
	Table string
	Size  string
}

type tableScanGen struct {
	Ref []tableScanGenRef
}

const tableScanTemplate = `
if (FILENAME == {{.Filename}}) {
  row_idx = {{.VarTableLength}};
  {{.VarTableLength}}++;

  field_count_tt = NF;
  {{if not .FullColumn }}
    field_count_tt = {{.MaxColumn}} < NF ? {{.MaxColumn}} : NF;
  {{end}}

  for (i = 0; i < field_count_tt; i++) {
    {{if .Filter != ""}}
    if (!({{.Filter}})) continue;
    {{end}}

    {{.VarTable}}[row_idx] = $i;
  }
  next;
}
`

func (self *tableScanGen) genOne(
	ts *sql.TableScan,
) (string, error) {
	filter, err := self.cg.genExpr(ts.Filter)
	if err != nil {
		return "", err
	}

	t, err := createTextTemplate(
		tableScanTemplate,
	)
	if err != nil {
		panic("codegen(TableScan): invalid template?")
	}

	out := &strings.Builder{}
	x := tableScanGenRef{
		Table: cfVarF("tbl", ts.Table.Index),
		Size:  cfVarF("tblsize", ts.Table.Index),
	}
	self.Ref = append(self.Ref, x)

	if err := t.Execute(out, map[string]interface{}{
		"Filename":       ts.Table.Path,
		"MaxColumn":      ts.Table.MaxColumn,
		"FullColumn":     ts.Table.FullColumn,
		"Filter":         filter,
		"VarTable":       x.Table,
		"VarTableLength": x.Size,
	}); err != nil {
		return "", err
	}
	return out.String(), nil
}

func (self *tableScanGen) Gen(
	p *plan.Plan,
) (string, error) {
	buf := &strings.Builder{}
	for _, ts := range p.TableScan {
		if data, err := self.genOne(ts); err != nil {
			return "", err
		} else {
			buf.WriteString(data)
		}
	}
	return buf.String(), nil
}
