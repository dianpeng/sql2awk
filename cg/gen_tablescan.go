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
    {{if ne .FS ""}}
    # workaround the issue with FS setting dynamically with awk. Additionally
    # workaround the split with regexp issue, notes the regexp is static
    if (FNR <= 1) {
      # always the first line have the issue, we will *NOT* use NF, but
      # use manual split hera, notes the FS will be treated as static regexp
      # whose type will not touch the split function implementation bug, which
      # not always use regexp search :(
      __workaround_sep_n = split($0, __workaround_sep, /{{.FS}}/);
      NF = __workaround_sep_n;
      for (__workaround_i = 1; __workaround_i <= NF; __workaround_i++) {
        $__workaround_i = __workaround_sep[__workaround_i];
      }
    }
    FS="{{.FS}}"
    {{end}}
    {{if gt .Start 0}}
    if (FNR <= {{.Start}}) {
      next;
    }
    {{end}}
    {{if gt .End 0}}
    if (FNR > {{.End}}) {
      nextfile;
    }
    {{end}}
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

func newtemplate(
	xx string,
) (*template.Template, error) {
	return template.New("[template]").Parse(xx)
}

func (self *tableScanGen) genOneTab(
	ts *plan.TableScan,
) string {
	filter := ""
	if ts.Filter != nil {
		filter = self.cg.genExpr(ts.Filter)
	}

	fs := ts.Table.Params.AsStr(0, "")
	start := ts.Table.Params.AsInt(1, -1)
	end := ts.Table.Params.AsInt(2, -1)

	t, err := newtemplate(
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
		"FS":             fs,
		"Start":          start,
		"End":            end,
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
		data := ""
		switch ts.Table.Type {
		case "tab", "Tab":
			data = self.genOneTab(ts)
			break

		default:
			panic("unknown table type")
			break
		}
		buf.WriteString(data)
	}
	return buf.String()
}
