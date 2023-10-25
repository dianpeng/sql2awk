package plan

import (
	"fmt"
	"github.com/dianpeng/sql2awk/sql"
)

func supportTab(
	name string,
) bool {
	switch name {
	case "tab", "tabular":
		return true // tabular data, default
	case "csv", "xsv":
		return true // comma separated data, not default, slow, but works :(
	default:
		return false
	}
}

// Try to resolve the symbol inside of the expression tree and generate some
// correct representation of the SQL tree. Part of the plan

func (self *Plan) genTableDescriptor(
	idx int,
	fromVar *sql.FromVar,
) (*TableDescriptor, error) {
	if len(fromVar.Vars) == 0 || fromVar.Vars[0].Ty != sql.ConstStr {
		return nil, self.err("scan-table", "table path must be specified")
	}
	if !supportTab(fromVar.Name) {
		return nil, self.err("scan-table", "unsupported table type")
	}

	rewrite, err := self.rewrite(fromVar.Rewrite)
	if err != nil {
		return nil, err
	}

	out := &TableDescriptor{
		Index:      idx,
		Path:       fromVar.Vars[0].String,
		Params:     sql.ConstList(fromVar.Vars[1:]),
		Type:       fromVar.Name,
		Alias:      fromVar.Alias,
		Options:    constListToOptions(fromVar.Vars[1:]),
		Symbol:     fmt.Sprintf("tbl_%d", idx),
		MaxColumn:  -1,
		Column:     make(map[int]bool),
		FullColumn: false,
		Rewrite:    rewrite,
	}

	return out, nil
}

func (self *Plan) findTableDescriptorByAlias(
	alias string,
) *TableDescriptor {
	for _, td := range self.tableList {
		if td.Alias == alias {
			return td
		}
	}
	return nil
}

func (self *Plan) indexTableDescriptor(
	idx int,
) *TableDescriptor {
	if idx >= len(self.tableList) {
		return nil
	} else {
		return self.tableList[idx]
	}
}

// Rewrite phase translation, kind of simple and intutitive
func (self *Plan) rewrite(r *sql.Rewrite) (*TableRewrite, error) {
	if r == nil {
		return nil, nil
	}
	out := &TableRewrite{}
	for _, rr := range r.List {
		if rc, err := self.rewriteOneClause(rr); err != nil {
			return nil, err
		} else {
			out.Stmt = append(out.Stmt, rc)
		}
	}
	return out, nil
}

func (self *Plan) rewriteOneClause(rr *sql.RewriteClause) (*TableRewriteStmt, error) {
	out := &TableRewriteStmt{
		Cond: rr.When,
	}
	for _, rr := range rr.Set {
		if set, err := self.rewriteSet(rr); err != nil {
			return nil, err
		} else {
			out.Set = append(out.Set, set)
		}
	}
	return out, nil
}

func (self *Plan) rewriteSet(rr *sql.RewriteSet) (*TableRewriteSet, error) {
	idx := self.codx(rr.Column)
	if idx < 0 {
		return nil, self.err("tablescan", "rewrite case's column index is invalid")
	}
	return &TableRewriteSet{
		Column: idx,
		Value:  rr.Value,
	}, nil
}

func (self *Plan) scanTable(s *sql.Select) error {
	if len(s.From.VarList) == 0 {
		return self.err("resolve-symbol", "no table specified?")
	}

	tableAlias := make(map[string]bool)

	// iterate through the *from* list to generate needed information
	for idx, fv := range s.From.VarList {
		if td, err := self.genTableDescriptor(idx, fv); err != nil {
			return err
		} else {
			if td.Alias != "" {
				if _, ok := tableAlias[td.Alias]; ok {
					return self.err("resolve-symbol", "table alias: %s already existed", td.Alias)
				}
			}
			tableAlias[td.Alias] = true
			self.tableList = append(self.tableList, td)
		}
	}

	if len(self.tableList) > self.Config.MaxTableSize {
		return self.err("resolve-symbol", "too many tables")
	}
	return nil
}
