package plan

// Semantic checking, just check obvious sql semantic bugs

// ----------------------------------------------------------------------------
//
// [1] analyze group by's expression
//
// 1) if it has aggregation expression, directly bailout with error
//
// 2) check the expression's access/reference of field index. Make sure that
//    all the selected vars/projection, does not show up in aggregation *MUST*
//    show up in group by
//
// [2] analyze wildcard. If wildcard is used, aggregation only shows up in
//     having, not projection, returns an error. Having cluase on a query
//     that does not have aggregation on the projection
//
// [3] aggregation function analyze, report error when its arity of parameters
//     is not expected
//
// ----------------------------------------------------------------------------

/*

type visitorHasAgg struct {
	hasAgg bool
}

func (self *visitorHasAgg) AcceptConst(*sql.Const) (bool, error) {
	return true, nil
}

func (self *visitorHasAgg) AcceptRef(*sql.Ref) (bool, error) {
	return true, nil
}

func (self *visitorHasAgg) AcceptSuffix(*sql.Suffix) (bool, error) {
	return true, nil
}

func (self *visitorHasAgg) AcceptPrimary(*sql.Primary) (bool, error) {
	idx, _, _, _ := self.isAggFunc(primary)
	if idx < 0 {
		return true, nil
	} else {
		self.hasAgg = true
		return false, nil
	}
}

func (self *visitorHasAgg) AcceptTernary(*sql.Ternary) (bool, error) {
	return true, nil
}

func (self *visitorHasAgg) AcceptBinary(*sql.Binary) (bool, error) {
	return true, nil
}

func (self *visitorHasAgg) AcceptUnary(*sql.Unary) (bool, error) {
	return true, nil
}

func (self *Plan) exprHasAgg(
	expr sql.Expr,
) bool {
	v := &visitorHasAgg{}
	sql.VisitExprPreOrder(v, expr)
	return v.hasAgg
}

func (self *Plan) anaGroupBy(s *sql.Select) error {
	groupBy := s.GroupBy
	if groupBy != nil {
		groupByInfo := newExprTableAccessSet()

		// check all the group by expression should not have aggregation
		for idx, v := range groupBy.Name {
			if self.exprHasAgg(v) {
				return self.err("sema", "[group_by]: $d'th expression has aggregation", idx)
			}
			groupByInfo.union(newExprTableAccessInfo(v).info)
		}

		// check all the accessed vars, that is not showed up inside of the agg must
		// shows up inside of group by
		if !s.Projection.HasWildcard() {
			projectInfo := newExprTableAccessSet()

			// collect all the projection value's table and field access info notes
			// this result in a set of sets
			for idx, v := range s.Projection.ValueList {
				if col, ok := v.(*sql.Col); ok {
					// check whether the Col's expression is aggregation or not
					if !self.exprHasAgg(col.Value) {
						projectInfo.union(newExprTableAccessInfo(col.Value).info)
					}
				}
			}

			// compare the set, the set result from the group by expression should
			// include the result result from projection filter
			if !groupByInfo.contain(projectInfo) {
				return self.err(
					"sema",
					"[group_by]: projected variable that is not in aggregation must be in group by",
				)
			}
		}
	} else {
		// if there's no group by, then all the projection variable must be in
		// aggregation format, otherwise invalid
		hasAgg := false
		hasNoneAgg := false

		for idx, v := range s.Projection.ValueList {
			if col, ok := v.(*sql.Col); ok {
				// check whether the Col's expression is aggregation or not
				if self.exprHasAgg(col.Value) {
					hasAgg = true
				} else {
					hasNoneAgg = true
				}
			}
		}

		if hasAgg && hasNoneAgg {
			return self.err(
				"sema",
				"[group_by]: group by is not specified, so all the projection must be in aggregation",
			)
		}
	}
	return nil
}

*/
