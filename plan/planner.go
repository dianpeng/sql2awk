package plan

import (
	"github.com/dianpeng/sql2awk/sql"
	"math"
)

func (self *Plan) planPrepare(s *sql.Select) error {
	// 1) scan table
	if err := self.scanTable(s); err != nil {
		return err
	}

	// 2) resolve all the symbols and resolve all the alias
	if err := self.resolveSymbol(s); err != nil {
		return err
	}

	// 3) analyze aggregation
	self.anaAgg(s)

	// 4) perform semantic check
	if err := self.semaCheck(s); err != nil {
		return err
	}
	return nil
}

// ----------------------------------------------------------------------------
// plan *table scan* node, kind of easy, just iterate all the table descriptor
// and try to use early filter, that's it
func (self *Plan) planTableScan(s *sql.Select) {
	var info *exprTableAccessInfo

	if s.Where != nil {
		info = newExprTableAccessInfo(s.Where.Condition)
	}

	for _, td := range self.tableList {
		var filter sql.Expr

		if info != nil {
			// try to obtain an early filter here
			filter = self.anaEarlyFilter(
				td.Index,
				info,
				s.Where.Condition,
			)
		}

		self.TableScan = append(self.TableScan, &TableScan{
			Table:  td,
			Filter: filter,
		})
	}
}

// ----------------------------------------------------------------------------
// plan join node

// plan join node's filter, this has some optimization internally
func (self *Plan) planJoinFilter(s *sql.Select) sql.Expr {
	if len(self.prune) == 0 {
		if s.Where == nil {
			return nil
		} else {
			return s.Where.Condition
		}
	}

	var cond sql.Expr
	for _, e := range self.unknownFilter(s.Where.Condition) {
		if cond == nil {
			cond = e
		} else {
			newNode := &sql.Binary{
				Op: sql.TkAnd,
				L:  cond,
				R:  e,
			}
			cond = newNode
		}
	}

	return cond
}

func (self *Plan) planJoin(s *sql.Select) {
	if self.HasJoin() {
		self.Join = &NestedLoopJoin{
			Filter: self.planJoinFilter(s),
		}
	}
}

// ----------------------------------------------------------------------------
// plan group by
func (self *Plan) planGroupBy(s *sql.Select) {
	if s.GroupBy != nil {
		self.GroupBy = &GroupBy{
			VarList: s.GroupBy.Name,
		}
	}
}

// ----------------------------------------------------------------------------
// plan aggregation
func (self *Plan) planAgg(s *sql.Select) {
	if self.HasAgg() {
		self.Agg = &Agg{
			VarList: self.aggExpr,
		}
	}
}

// ----------------------------------------------------------------------------
// plan having
func (self *Plan) planHaving(s *sql.Select) {
	if s.Having != nil {
		self.Having = &Having{
			Filter: s.Having.Condition,
		}
	}
}

// ----------------------------------------------------------------------------
// plan output
// output is lies inside of the SelectVar, just use select var will be fine,
// ie perform some expression operation

func (self *Plan) isTableWildcard(
	col *sql.Col,
) *TableDescriptor {
	v := col.Value
	if v.Type() == sql.ExprPrimary {
		primary := v.(*sql.Primary)

		if primary.CanName.IsTableColumn() &&
			primary.CanName.ColumnIndex == WildcardColumnIndex {
			// just table index is valid, okay it is a wildcard
			return self.tableList[primary.CanName.TableIndex]
		}
	}
	return nil
}

func (self *Plan) planOutput(s *sql.Select) {
	// 1) projection part, needs to take care of

	//    iterate through projection variables and collect how the output are
	//    supposed to be displayed
	self.Output = &Output{}

	for _, x := range s.Projection.ValueList {
		switch x.Type() {
		case sql.SelectVarCol:
			col := x.(*sql.Col)
			value := col.Value

			// The following logic is kind of tricky, mostly because of the variation
			// SQL syntax. The key here is that primary will have all the needed
			// information inside of the CanName. Based on CanName's value we are
			// supposed to generate the corresponding OutputVar. The syntax is kind
			// of messy for SQL ...
			switch value.Type() {
			case sql.ExprPrimary:
				primary := value.(*sql.Primary)

				if primary.CanName.IsMatcher() {
					// If it is a matcher, we are sure that it is either a Column
					// or row matcher
					tidx := primary.CanName.TableIndex
					sym := primary.CanName.Symbol
					pattern := primary.CanName.Pattern

					addCol := func(idx int) {
						self.Output.VarList.addColMatch(
							self.tableList[idx],
							pattern,
							col.Alias(),
						)
						self.TableScan[idx].ColFilter = &TableMatcher{
							Match:   OutputVarColMatch,
							Pattern: pattern,
						}
					}

					addRow := func(idx int) {
						self.Output.VarList.addRowMatch(
							self.tableList[idx],
							pattern,
							col.Alias(),
						)
						self.TableScan[idx].RowFilter = &TableMatcher{
							Match:   OutputVarRowMatch,
							Pattern: pattern,
						}
					}

					if tidx == wildcardTableIndex {
						// wildcard table, which means *all* the table will use this one
						// we need to generate a matcher *for all the table scan*
						for idx, _ := range self.TableScan {
							switch sym {
							case sql.SymbolColumns:
								addCol(idx)
								break

							default:
								addRow(idx)
								break
							}
						}
					} else {
						if sym == sql.SymbolColumns {
							addCol(tidx)
						} else {
							addRow(tidx)
						}
					}
				} else if primary.CanName.IsTableColumn() {
					tidx := primary.CanName.TableIndex
					if primary.CanName.ColumnIndex == WildcardColumnIndex {
						self.Output.VarList.addWildcard(
							self.tableList[tidx],
							col.Alias(),
						)
					} else {
						self.Output.VarList.addValue(
							col.Value,
							col.Alias(),
						)
					}
				} else {
					self.Output.VarList.addValue(
						col.Value,
						col.Alias(),
					)
				}
				break
			default:
				self.Output.VarList.addValue(
					col.Value,
					col.Alias(),
				)
				break
			}

			break

		case sql.SelectVarStar:
			self.Output.Wildcard = true
			break

		default:
			break
		}
	}

	// 2) Output
	if !self.Output.Wildcard {
		self.Output.VarSize = len(self.Output.VarList)
	} else {
		self.Output.VarSize = self.totalTableColumnSize()
	}

	// 3) Distinct
	self.Output.Distinct = s.Distinct

	// 4) Limit
	if s.Limit != nil {
		self.Output.Limit = s.Limit.Limit
	} else {
		self.Output.Limit = math.MaxInt64
	}
}

// ----------------------------------------------------------------------------
// plan the sorting. which is not really SQL's builtin sort, since AWK does
// not have sorting for now ... we may use gawk's sort internally which will
// be better obviously
func (self *Plan) planSort(s *sql.Select) {
	if s.OrderBy != nil {
		asc := false
		if s.OrderBy.Order == sql.OrderAsc {
			asc = true
		}
		self.Sort = &Sort{
			Asc:     asc,
			VarList: s.OrderBy.Name,
		}
	}
}

func (self *Plan) plan(s *sql.Select) error {
	if err := self.planPrepare(s); err != nil {
		return err
	}
	self.planTableScan(s)
	self.planJoin(s)
	self.planGroupBy(s)
	self.planAgg(s)
	self.planHaving(s)
	self.planSort(s)
	self.planOutput(s)
	if err := self.planFormat(s); err != nil {
		return err
	}
	return nil
}
