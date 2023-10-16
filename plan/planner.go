package plan

import (
	"github.com/dianpeng/sql2awk/sql"
	"math"
)

func (self *Plan) planPrepare(s *sql.Select) {
	// 1) resolve all the symbols and resolve all the alias
	self.resolveSymbol(s)

	// 2) analyze aggregation
	self.anaAgg(s)
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
		if !s.Projection.HasWildcard() && td.IsDangling() {
			continue
		}
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
func (self *Plan) planOutput(s *sql.Select) {
	// 1) projection part, needs to take care of

	//    iterate through projection variables and collect how the output are
	//    supposed to be displayed
	self.Output = &Output{}

	for _, x := range s.Projection.ValueList {
		switch x.Type() {
		case sql.SelectVarCol:
			col := x.(*sql.Col)
			self.Output.VarList = append(self.Output.VarList, col.Value)
			break

		case sql.SelectVarStar:
			self.Output.Wildcard = true
			break

		default:
			break
		}
	}

	// 2) Sorting variable needs to be output as well, since we need to use
	//    sort command line
	if s.OrderBy != nil {
		self.Output.SortList = s.OrderBy.Name
	}

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
			Asc:    asc,
			Offset: self.Output.VarSize,
		}
	}
}

func (self *Plan) plan(s *sql.Select) {
	self.planPrepare(s)
	self.planTableScan(s)
	self.planJoin(s)
	self.planGroupBy(s)
	self.planAgg(s)
	self.planHaving(s)
	self.planOutput(s)
	self.planSort(s)
}
