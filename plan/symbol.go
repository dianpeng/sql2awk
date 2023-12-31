package plan

import (
	"github.com/dianpeng/sql2awk/sql"
)

type visitorResolveSymbol struct {
	p *Plan
}

func (self *visitorResolveSymbol) resolveSymbolExprSuffix(
	leading sql.Expr,
	component string,
	symbol int,
	cn *sql.CanName,
) error {
	if leading.Type() != sql.ExprRef {
		return self.p.err("resolve-symbol", "unknown full table qualified column name")
	}
	tableName := leading.(*sql.Ref).Id // table name
	tableDesp := self.p.findTableDescriptorByAlias(tableName)
	if tableDesp == nil {
		return self.p.err("resolve-symbol", "unknown table: %s", tableName)
	}

	colIdx := 0
	switch symbol {
	case sql.SymbolNone:
		colIdx = self.p.codx(component) // column index
		if colIdx < 0 {
			return self.p.err("resolve-symbol", "invalid field name, must be $XX")
		}
		break

	case sql.SymbolStar:
		colIdx = WildcardColumnIndex
		tableDesp.SetFullColumn(self.p.Config.MaxColumnSize)
		break

	default:
		return self.p.err("resolve-symbol", "invalid field name, invalid symbol")
	}

	cn.Set(tableDesp.Index, colIdx)
	tableDesp.UpdateColumnIndex(colIdx)
	return nil
}

func (self *visitorResolveSymbol) resolveSymbolExprSuffixDot(
	dotPrimary *sql.Primary,
) error {
	return self.resolveSymbolExprSuffix(
		dotPrimary.Leading,
		dotPrimary.Suffix[0].Component,
		dotPrimary.Suffix[0].Symbol,
		&dotPrimary.CanName,
	)
}

func (self *visitorResolveSymbol) resolveSymbolExprSuffixIndex(
	dotPrimary *sql.Primary,
) error {
	return self.p.err("resolve-symbol", "cannot use []/index operator here in projection")
}

func (self *visitorResolveSymbol) AcceptRef(
	ref *sql.Ref,
) (bool, error) {
	if colIdx := self.p.codx(ref.Id); colIdx >= 0 {
		self.p.tableList[0].UpdateColumnIndex(colIdx)
		ref.CanName.Set(0, colIdx)
	}
	return true, nil
}

func (self *visitorResolveSymbol) AcceptConst(
	*sql.Const,
) (bool, error) {
	return true, nil
}

func (self *visitorResolveSymbol) AcceptUnary(
	*sql.Unary,
) (bool, error) {
	return true, nil
}

func (self *visitorResolveSymbol) AcceptBinary(
	*sql.Binary,
) (bool, error) {
	return true, nil
}

func (self *visitorResolveSymbol) AcceptTernary(
	*sql.Ternary,
) (bool, error) {
	return true, nil
}

func (self *visitorResolveSymbol) resolveSymbolExprSuffixTableMatcher(
	primary *sql.Primary,
) error {
	// this *MUST BE* a row/column filter, which has special Symbol during the
	// parser.
	leading := primary.Leading

	if leading.Type() != sql.ExprRef {
		return self.p.err(
			"resolve-symbol",
			"table level pattern matcher, unknown selector",
		)
	}
	ref := leading.(*sql.Ref)
	var pattern *sql.Suffix
	tidx := 0
	sym := 0

	switch ref.Symbol {
	case sql.SymbolColumns, sql.SymbolRows:
		sym = ref.Symbol
		if len(primary.Suffix) != 1 {
			return self.p.err(
				"resolve-symbol",
				"table level pattern matcher, COLUMNS/ROWS invalid suffix expression",
			)
		}
		pattern = primary.Suffix[0]
		tidx = wildcardTableIndex
		self.p.setAllTableFullColumn()
		break

	case sql.SymbolNone:
		if len(primary.Suffix) == 2 {
			colOrRow := primary.Suffix[0]
			if colOrRow.Ty != sql.SuffixDot &&
				(colOrRow.Symbol != sql.SymbolColumns && colOrRow.Symbol != sql.SymbolRows) {
				return self.p.err(
					"resolve-symbol",
					"table level pattern matcher, ROWS/COLUMNS keyword must follow table selector",
				)
			}
			sym = colOrRow.Symbol
			pattern = primary.Suffix[1]
			tableName := ref.Id
			tableDesp := self.p.findTableDescriptorByAlias(tableName)
			if tableDesp == nil {
				return self.p.err("resolve-symbol", "unknown table: %s", tableName)
			}
			tidx = tableDesp.Index
			tableDesp.SetFullColumn(self.p.Config.MaxColumnSize)
		} else {
			return nil // just a function call
		}
		break

	default:
		return self.p.err(
			"resolve-symbol",
			"table level pattern matcher, unknown symbol",
		)
	}

	if pattern.Ty != sql.SuffixCall || len(pattern.Call.Parameters) != 1 {
		return self.p.err(
			"resolve-symbol",
			"table level pattern matcher, ROWS/COLUMNS keyword must be a call with "+
				"regex pattern as its only parameters",
		)
	}

	if pexpr := pattern.Call.Parameters[0]; pexpr.Type() != sql.ExprConst || pexpr.(*sql.Const).Ty != sql.ConstStr {
		return self.p.err(
			"resolve-symbol",
			"table level pattern matcher, ROWS/COLUMNS keyword must be a call with "+
				"string parameter served as regex pattern",
		)
	}

	primary.CanName.SetMatcher(
		tidx,
		sym,
		pattern.Call.Parameters[0].(*sql.Const).String, // pattern
	)
	return nil
}

func (self *visitorResolveSymbol) AcceptPrimary(
	primary *sql.Primary,
) (bool, error) {

	// Arity check here, we should do this maybe inside of semantic checking but
	// kind of hard to separate sinec the sema check happened *after* the symbol
	// resolution phase.
	//
	// For now, the only allowed suffix expression is as following
	// 1) a.b, notes a.b.c is impossible since we do not have nested table row
	// 2) a(), which is function call or aggregation function call
	// 3) a.b(...), this is possible since we allow special column/row matching
	//    syntax, ie t1.COLUMNS("regex"), t1.ROWS("regex"),
	// 4) a.*, this means for table a, select all its columns
	//
	// above all, the arity of the primary can only be at most 2

	if len(primary.Suffix) > 2 {
		// this is impossible for now, since our SQL does not allow list/map
		// compound type and subscript of these types
		return false, self.p.err("resolve-symbol", "invalid suffix expression nesting")
	}

	suffixLen := len(primary.Suffix)
	switch suffixLen {
	default:
		break

	case 1:
		suff := primary.Suffix[0]
		switch suff.Ty {
		case sql.SuffixDot:
			// a dotish reference, treat it as a qualified table column reference
			if err := self.resolveSymbolExprSuffixDot(primary); err != nil {
				return false, err
			}
			break
		case sql.SuffixIndex:
			// a indexish reference, treat it as a qualified table column reference
			if err := self.resolveSymbolExprSuffixIndex(primary); err != nil {
				return false, err
			}
			break
		case sql.SuffixCall:
			// check wildcard COLUMNS/ROWS special syntax
			if err := self.resolveSymbolExprSuffixTableMatcher(primary); err != nil {
				return false, err
			}
			break

		default:
			break
		}
		break

	case 2:
		if err := self.resolveSymbolExprSuffixTableMatcher(primary); err != nil {
			return false, err
		}
		break
	}

	return true, nil
}

func (self *visitorResolveSymbol) AcceptSuffix(
	*sql.Suffix,
) (bool, error) {
	return true, nil
}

func (self *Plan) resolveSymbolExpr(
	expr sql.Expr,
) error {
	return sql.VisitExprPreOrder(
		&visitorResolveSymbol{
			p: self,
		},
		expr,
	)
}

func (self *Plan) canonicalize(s *sql.Select) error {
	// 1) try to visit each expression tree to resolve the symbol, or generate an
	//    error. Notes this will leave unknow symbol untouch, until we resolve all
	//    the alias afterwards

	// 1.1) Projections
	if len(s.Projection.ValueList) == 1 &&
		s.Projection.ValueList[0].Type() == sql.SelectVarStar {
		// quick check for *star* style select, ie select * from xxxxx, etc ...
		// we will have to update *all*
		self.setAllTableFullColumn()
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
	} else if id != "*" {
		// notes: we use * Ref as a special way to mark that wildcard parameter
		//   for aggregation parameter, which we may need to fix it in the future
		return self.err("resolve-symbol", "id: %s is unknown", id)
	}
	return nil
}

type visitorAlias struct {
	p *Plan
}

func (self *visitorAlias) AcceptRef(
	ref *sql.Ref,
) (bool, error) {
	if err := self.p.resolveAliasId(ref.Id, &ref.CanName); err != nil {
		return false, err
	}
	return true, nil
}

func (self *visitorAlias) AcceptPrimary(
	primary *sql.Primary,
) (bool, error) {
	if suff := primary.Suffix[0]; suff.Ty == sql.SuffixCall {
		// NOTES(dpeng):
		//   We only want to *partially* visit the expression instead of fully,
		//   so we do the visiting by ourself instead of letting the visitor do so
		if err := sql.VisitExprPreOrder(
			&visitorAlias{
				p: self.p,
			},
			suff,
		); err != nil {
			return false, err
		}
	}

	// do not visit primary expression
	return false, nil
}

func (self *visitorAlias) AcceptConst(
	*sql.Const,
) (bool, error) {
	return true, nil
}

func (self *visitorAlias) AcceptSuffix(
	*sql.Suffix,
) (bool, error) {
	return true, nil
}

func (self *visitorAlias) AcceptUnary(
	*sql.Unary,
) (bool, error) {
	return true, nil
}

func (self *visitorAlias) AcceptBinary(
	*sql.Binary,
) (bool, error) {
	return true, nil
}

func (self *visitorAlias) AcceptTernary(
	*sql.Ternary,
) (bool, error) {
	return true, nil
}

// FIXME(dpeng): implement visitor for AST
func (self *Plan) resolveAliasExpr(expr sql.Expr) error {
	return sql.VisitExprPreOrder(
		&visitorAlias{
			p: self,
		},
		expr,
	)
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

func (self *Plan) scanTableAndResolveSymbol(s *sql.Select) error {
	if err := self.scanTable(s); err != nil {
		return err
	}
	return self.resolveSymbol(s)
}

func (self *Plan) resolveSymbol(s *sql.Select) error {
	// 1) resolve symbol to its canonicalized name if we can, ie basically resove
	//    any dot suffix expression to be full name
	if err := self.canonicalize(s); err != nil {
		return err
	}

	// 2) resolve symbol's alias or global variables
	if err := self.resolveAlias(s); err != nil {
		return err
	}

	return nil
}
