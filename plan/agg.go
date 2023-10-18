package plan

import (
	"github.com/dianpeng/sql2awk/sql"
	"strings"
)

// Analyzing the aggregation function inside of an expression.
//
// The aggregation expression can show up almost anywhere inside of SQL
//
// 1) projection
// 2) having
// 3) order by, even ...
//
// The aggregation expression will be resolved during Agg kicks in, which has
// 2 possible position, one is after the group by and the other is standalone.
//
// Additionally, aggregation expression will have additional expression depend
// on it, ie MIN(a+b) + c, the last '+ c' requires the MIN(a+b) is done. To
// address these complication, we have a phase as following. We rewrite all
// the aggregation expression, recognized by the following analyze, to a local
// variable reference. We do all the aggregation expression in the aggregation
// phase and store the related aggregation result into temporary variable along
// with each record/column. And replace all the node of aggregation node in
// AST become reference to local variable. This process will mutate *ALL* the
// cluase which may have aggregation function.

// try to analyze the input expression to check whether it contains an agg
// expression or not. If so, split the expression tree as following,
func (self *Plan) anaAgg(
	s *sql.Select,
) error {
	// projection
	for _, svar := range s.Projection.ValueList {
		col, ok := svar.(*sql.Col)
		if ok {
			if err := self.anaAggExpr(col.Value); err != nil {
				return err
			}
		}
	}

	// having
	if s.Having != nil {
		if err := self.anaAggExpr(s.Having.Condition); err != nil {
			return err
		}
	}

	// order by
	if s.OrderBy != nil {
		for _, v := range s.OrderBy.Name {
			if err := self.anaAggExpr(v); err != nil {
				return err
			}
		}
	}

	return nil
}

// We visit an expression tree in DFS order, until we hit a suffix expression
// with known AGG style syntax, ie MIN/MAX/AVG/SUM/COUNT/XXX(expression). Then
// we convert it into a AggVar object, with the expression that gonna be AGGed
// as Var inside of AggVar and AggType set to correct agg expression type.
// For its parental node, ie who reference it, will be come a reference to a
// agg internal node. The information is stored directly inside of Primary's
// CanName field.
func (self *Plan) isAggFunc(
	p *sql.Primary,
) (int, sql.Expr, sql.Expr, error) {
	if p.Leading.Type() != sql.ExprRef {
		return -1, nil, nil, nil
	}
	ty := -1

	switch strings.ToLower(p.Leading.(*sql.Ref).Id) {
	case "min":
		ty = AggMin
		break

	case "max":
		ty = AggMax
		break

	case "avg":
		ty = AggAvg
		break

	case "sum":
		ty = AggSum
		break

	case "count":
		ty = AggCount
		break

	case "percentile":
		ty = AggPercentile
		break

	case "histogram":
		ty = AggHistogram
		break

	default:
		return -1, nil, nil, nil
	}

	// now check arity of parameters, if arity does not match, we do not treat
	// them *as* aggregation function either
	if len(p.Suffix) != 1 {
		return -1, nil, nil, self.err("agg", "invalid arity for aggregation function")
	}
	if p.Suffix[0].Ty != sql.SuffixCall {
		return -1, nil, nil, self.err("agg", "aggregation must be function call")
	}
	if len(p.Suffix[0].Call.Parameters) == 0 {
		return -1, nil, nil, self.err(
			"agg",
			"aggregation function must have at least one parameters",
		)
	}

	return ty, p.Suffix[0], p.Suffix[0].Call.Parameters[0], nil
}

type visitorTransAgg struct {
	p *Plan
}

func (self *visitorTransAgg) AcceptPrimary(
	primary *sql.Primary,
) (bool, error) {
	ty, inner, target, err := self.p.isAggFunc(primary)
	if err != nil {
		return false, err
	}

	if ty == -1 {
		return true, nil
	}

	// 1) record agg expression
	avar := AggVar{
		AggType: ty,
		Value:   inner,
		Target:  target,
	}

	idx := len(self.p.aggExpr)
	self.p.aggExpr = append(self.p.aggExpr, avar)

	// 2) mutate the current primary node's CanName
	primary.CanName.Set(
		aggTableIndex,
		idx,
	)

	return false, nil
}

func (self *visitorTransAgg) AcceptConst(*sql.Const) (bool, error) {
	return true, nil
}

func (self *visitorTransAgg) AcceptRef(*sql.Ref) (bool, error) {
	return true, nil
}

func (self *visitorTransAgg) AcceptSuffix(*sql.Suffix) (bool, error) {
	return true, nil
}

func (self *visitorTransAgg) AcceptTernary(*sql.Ternary) (bool, error) {
	return true, nil
}

func (self *visitorTransAgg) AcceptBinary(*sql.Binary) (bool, error) {
	return true, nil
}

func (self *visitorTransAgg) AcceptUnary(*sql.Unary) (bool, error) {
	return true, nil
}

func (self *Plan) anaAggExpr(
	expr sql.Expr,
) error {
	return sql.VisitExprPreOrder(
		&visitorTransAgg{
			p: self,
		},
		expr,
	)
}

type visitorHasAgg struct {
	p      *Plan
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

func (self *visitorHasAgg) AcceptPrimary(primary *sql.Primary) (bool, error) {
	idx, _, _, _ := self.p.isAggFunc(primary)
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
	v := &visitorHasAgg{
		p: self,
	}
	sql.VisitExprPreOrder(v, expr)
	return v.hasAgg
}
