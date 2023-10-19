package plan

import (
	"fmt"
	"github.com/dianpeng/sql2awk/sql"
)

// Try to resolve the symbol inside of the expression tree and generate some
// correct representation of the SQL tree. Part of the plan

func (self *Plan) genTableDescriptor(
	idx int,
	fromVar *sql.FromVar,
) (*TableDescriptor, error) {
	if len(fromVar.Vars) == 0 || fromVar.Vars[0].Ty != sql.ConstStr {
		return nil, self.err("scan-table", "table path must be specified")
	}
	if fromVar.Name != "tab" && fromVar.Name != "tabular" {
		return nil, self.err("scan-table", "unknown table type")
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
