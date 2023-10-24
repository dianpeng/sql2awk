package plan

import (
	"github.com/dianpeng/sql2awk/sql"
)

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
func (self *Plan) semaCheckGroupBy(s *sql.Select) error {
	groupBy := s.GroupBy
	if groupBy != nil {
		groupByInfo := newExprTableAccessSet()

		// check all the group by expression should not have aggregation
		for idx, v := range groupBy.Name {
			if self.exprHasAgg(v) {
				return self.err("sema", "[group_by]: %d'th expression has aggregation", idx)
			}
			groupByInfo.union(getExprTableAccessSet(v))
		}

		// check all the accessed vars, that is not showed up inside of the agg must
		// shows up inside of group by
		if !s.Projection.HasStar() {
			projectInfo := newExprTableAccessSet()

			// collect all the projection value's table and field access info notes
			// this result in a set of sets
			for _, v := range s.Projection.ValueList {
				if col, ok := v.(*sql.Col); ok {
					// check whether the Col's expression is aggregation or not
					if !self.exprHasAgg(col.Value) {
						projectInfo.union(getExprTableAccessSet(col.Value))
					}
				}
			}

			// compare the set, the set result from the group by expression should
			// include the result result from projection filter
			if !groupByInfo.contain(projectInfo) {
				return self.err(
					"sema",
					"[group_by]: projected variable that is not in aggregation must "+
						"be in group by",
				)
			}
		}
	} else {
		// if there's no group by, then all the projection variable must be in
		// aggregation format, otherwise invalid
		hasAgg, hasNoneAgg := self.semaCheckProjectionAgg(s.Projection)

		if hasAgg && hasNoneAgg {
			return self.err(
				"sema",
				"[group_by]: group by is not specified, but projection var contains "+
					"mixed aggregation with none aggregation expression",
			)
		}
	}
	return nil
}

func (self *Plan) semaCheckProjectionAgg(
	projection *sql.Projection,
) (bool, bool) {
	hasAgg := false
	hasNoneAgg := false
	for _, v := range projection.ValueList {
		if col, ok := v.(*sql.Col); ok {
			// check whether the Col's expression is aggregation or not
			if self.exprHasAgg(col.Value) {
				hasAgg = true
			} else {
				hasNoneAgg = true
			}
		}
	}
	return hasAgg, hasNoneAgg
}

func (self *Plan) semaCheckHavingAgg(
	s *sql.Having,
) bool {
	if s == nil {
		return false
	}
	return self.exprHasAgg(s.Condition)
}

func (self *Plan) semaCheckWildcard(s *sql.Select) error {
	if s.Projection.HasStar() {
		projectionHasAgg, _ := self.semaCheckProjectionAgg(s.Projection) // projection has agg
		havingHasAgg := self.semaCheckHavingAgg(s.Having)                // having has agg
		if havingHasAgg && !projectionHasAgg {
			return self.err(
				"sema",
				"[wildcard]: A none aggregation query with having statement contains "+
					"aggregation is invalid",
			)
		}
	}

	return nil
}

func (self *Plan) semaCheck(s *sql.Select) error {
	if err := self.semaCheckGroupBy(s); err != nil {
		return err
	}
	if err := self.semaCheckWildcard(s); err != nil {
		return err
	}

	return nil
}
