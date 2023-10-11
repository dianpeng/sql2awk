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
		return nil, self.err("resolve-symbol", "table path must be specified")
	}

	out := &TableDescriptor{
		Index:      idx,
		Path:       fromVar.Vars[0].String,
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

func (self *Plan) resolveSymbolExprSuffix(
	leading sql.Expr,
	component string,
	cn *sql.CanName,
) error {
	if leading.Type() != sql.ExprRef {
		return self.err("resolve-symbol", "unknown full table qualified column name")
	}
	tableName := leading.(*sql.Ref).Id // table name
	colIdx := self.codx(component)     // column index
	if colIdx < 0 {
		return self.err("resolve-symbol", "invalid field name, must be $XX")
	}
	tableDesp := self.findTableDescriptorByAlias(tableName)
	if tableDesp == nil {
		return self.err("resolve-symbol", "unknown table: %s", tableName)
	}

	cn.Set(tableDesp.Index, colIdx)
	tableDesp.UpdateColumnIndex(colIdx)
	return nil
}

func (self *Plan) resolveSymbolExprSuffixDot(
	dotPrimary *sql.Primary,
) error {
	return self.resolveSymbolExprSuffix(
		dotPrimary.Leading,
		dotPrimary.Suffix[0].Component,
		&dotPrimary.CanName,
	)
}

func (self *Plan) resolveSymbolExprSuffixIndex(
	dotPrimary *sql.Primary,
) error {
	return self.err("resolve-symbol", "cannot use []/index operator here in projection")
}

func (self *Plan) resolveSymbolExprRef(
	ref *sql.Ref,
) error {

	if colIdx := self.codx(ref.Id); colIdx >= 0 {
		self.tableList[0].UpdateColumnIndex(colIdx)
		ref.CanName.Set(0, colIdx)
	}

	// leave it as it is, since the *as/alias* will also need to resolve *after*
	// this resolution
	return nil
}

func (self *Plan) resolveSymbolExpr(
	expr sql.Expr,
) error {
	switch expr.Type() {
	default:
		break

	case sql.ExprRef:
		ref := expr.(*sql.Ref)
		if err := self.resolveSymbolExprRef(ref); err != nil {
			return err
		}
		break

	case sql.ExprSuffix:
		suffix := expr.(*sql.Suffix)
		switch suffix.Ty {
		case sql.SuffixCall:
			for _, x := range suffix.Call.Parameters {
				if err := self.resolveSymbolExpr(x); err != nil {
					return err
				}
			}
			break

		case sql.SuffixIndex:
			return self.resolveSymbolExpr(suffix.Index)

		default:
			break
		}
		break

	case sql.ExprPrimary:
		primary := expr.(*sql.Primary)

		if len(primary.Suffix) >= 2 {
			// this is impossible for now, since our SQL does not allow list/map
			// compound type and subscript of these types
			return self.err("resolve-symbol", "invalid suffix expression nesting")
		}

		if len(primary.Suffix) == 1 {
			suff := primary.Suffix[0]
			switch suff.Ty {
			case sql.SuffixDot:
				// a dotish reference, treat it as a qualified table column reference
				if err := self.resolveSymbolExprSuffixDot(primary); err != nil {
					return err
				}
				break
			case sql.SuffixIndex:
				// a indexish reference, treat it as a qualified table column reference
				if err := self.resolveSymbolExprSuffixIndex(primary); err != nil {
					return err
				}
				break
			default:
				// do nothing for the leading part of the call, the leading part must be
				// a ref though
				if err := self.resolveSymbolExpr(suff); err != nil {
					return err
				}
				break
			}
		} else {
			return self.resolveSymbolExpr(primary.Leading)
		}
		break

	case sql.ExprUnary:
		unary := expr.(*sql.Unary)
		return self.resolveSymbolExpr(unary.Operand)

	case sql.ExprBinary:
		binary := expr.(*sql.Binary)
		if err := self.resolveSymbolExpr(binary.L); err != nil {
			return err
		}
		if err := self.resolveSymbolExpr(binary.R); err != nil {
			return err
		}
		break

	case sql.ExprTernary:
		ternary := expr.(*sql.Ternary)
		if err := self.resolveSymbolExpr(ternary.Cond); err != nil {
			return err
		}
		if err := self.resolveSymbolExpr(ternary.B0); err != nil {
			return err
		}
		if err := self.resolveSymbolExpr(ternary.B1); err != nil {
			return err
		}
		break
	}
	return nil
}

func (self *Plan) canonicalize(s *sql.Select) error {
	// 1) try to visit each expression tree to resolve the symbol, or generate an
	//    error. Notes this will leave unknow symbol untouch, until we resolve all
	//    the alias afterwards

	// 1.1) Projections
	if len(s.Projection.ValueList) == 1 && s.Projection.ValueList[0].Type() == sql.SelectVarStar {
		// quick check for *star* style select, ie select * from xxxxx, etc ...
		// we will have to update *all*
		for _, x := range self.tableList {
			x.SetFullColumn()
		}
	} else {
		for _, x := range s.Projection.ValueList {
			col := x.(*sql.Col) // must be col
			if err := self.resolveSymbolExpr(col.Value); err != nil {
				return err
			}
		}
	}

	// 1.2) Where
	if s.Where != nil {
		if err := self.resolveSymbolExpr(s.Where.Condition); err != nil {
			return err
		}
	}

	// 1.3) Group by
	if s.GroupBy != nil {
		for _, cn := range s.GroupBy.Name {
			if err := self.resolveSymbolExpr(cn); err != nil {
				return err
			}
		}
	}

	// 1.4) Having
	if s.Having != nil {
		if err := self.resolveSymbolExpr(s.Having.Condition); err != nil {
			return err
		}
	}

	// 1.5) Order by
	if s.OrderBy != nil {
		for _, cn := range s.OrderBy.Name {
			if err := self.resolveSymbolExpr(cn); err != nil {
				return err
			}
		}
	}

	return nil
}

func (self *Plan) setupAlias(projection *sql.Projection) error {
	for _, x := range projection.ValueList {
		if x.Alias() != "" {
			col := x.(*sql.Col)
			if _, ok := self.alias[x.Alias()]; ok {
				return self.err("resolve-symbol", "alias: %s already existed", x.Alias())
			}
			self.alias[x.Alias()] = col.Value
		}
	}
	return nil
}

func (self *Plan) findAlias(id string) sql.Expr {
	return self.alias[id]
}

func (self *Plan) resolveAliasId(id string, cn *sql.CanName) error {
	if cn.IsSettled() {
		return nil // do nothing, if the alias already been settled
	}

	if alias := self.findAlias(id); alias != nil {
		cn.SetRef(alias)
	} else if self.isGlobalVariable(id) {
		cn.SetGlobal()
	} else {
		return self.err("resolve-symbol", "id: %s is unknown", id)
	}
	return nil
}

// FIXME(dpeng): implement visitor for AST
func (self *Plan) resolveAliasExpr(expr sql.Expr) error {
	switch expr.Type() {
	default:
		break

	case sql.ExprRef:
		ref := expr.(*sql.Ref)
		if err := self.resolveAliasId(ref.Id, &ref.CanName); err != nil {
			return err
		}
		break

	case sql.ExprSuffix:
		suffix := expr.(*sql.Suffix)
		switch suffix.Ty {
		case sql.SuffixCall:
			for _, x := range suffix.Call.Parameters {
				if err := self.resolveAliasExpr(x); err != nil {
					return err
				}
			}
			break

		case sql.SuffixIndex:
			return self.resolveAliasExpr(suffix.Index)

		default:
			break
		}
		break

	case sql.ExprPrimary:
		// call requires resolution
		primary := expr.(*sql.Primary)
		suff := primary.Suffix[0]
		if suff.Ty == sql.SuffixCall {
			return self.resolveAliasExpr(suff)
		}
		break

	case sql.ExprUnary:
		unary := expr.(*sql.Unary)
		return self.resolveAliasExpr(unary.Operand)

	case sql.ExprBinary:
		binary := expr.(*sql.Binary)
		if err := self.resolveAliasExpr(binary.L); err != nil {
			return err
		}
		if err := self.resolveAliasExpr(binary.R); err != nil {
			return err
		}
		break

	case sql.ExprTernary:
		ternary := expr.(*sql.Ternary)
		if err := self.resolveAliasExpr(ternary.Cond); err != nil {
			return err
		}
		if err := self.resolveAliasExpr(ternary.B0); err != nil {
			return err
		}
		if err := self.resolveAliasExpr(ternary.B1); err != nil {
			return err
		}
		break
	}
	return nil
}

func (self *Plan) resolveAlias(s *sql.Select) error {
	// setup alias table, otherwise failed with error
	if err := self.setupAlias(s.Projection); err != nil {
		return err
	}

	// go through each component of select to finally resolve/settle down the
	// symbol, otherwise failed
	for _, p := range s.Projection.ValueList {
		col, ok := p.(*sql.Col)
		if ok {
			if err := self.resolveAliasExpr(col.Value); err != nil {
				return err
			}
		}
	}

	// where clause
	if s.Where != nil {
		if err := self.resolveAliasExpr(s.Where.Condition); err != nil {
			return err
		}
	}

	// group by
	if s.GroupBy != nil {
		for _, cn := range s.GroupBy.Name {
			if err := self.resolveAliasExpr(cn); err != nil {
				return err
			}
		}
	}

	// having
	if s.Having != nil {
		if err := self.resolveAliasExpr(s.Having.Condition); err != nil {
			return err
		}
	}

	// order by
	if s.OrderBy != nil {
		for _, cn := range s.OrderBy.Name {
			if err := self.resolveAliasExpr(cn); err != nil {
				return err
			}
		}
	}

	return nil
}

func (self *Plan) resolveSymbol(s *sql.Select) error {
	// 1) generate table descriptor based on FROM clause, and name each table
	//    accordingly
	if err := self.scanTable(s); err != nil {
		return err
	}

	// 2) resolve symbol to its canonicalized name if we can, ie basically resove
	//    any dot suffix expression to be full name
	if err := self.canonicalize(s); err != nil {
		return err
	}

	// 3) resolve symbol's alias or global variables
	if err := self.resolveAlias(s); err != nil {
		return err
	}

	return nil
}
