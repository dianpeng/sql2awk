package plan

import (
	"github.com/dianpeng/sql2awk/sql"
	"sort"
)

// ----------------------------------------------------------------------------
//
// Early filter is a simple optimization that we try to perform. Basically, we
// try to *split* the where clause condition into 2 parts, W.R.T each table.
// For each table, we use a simple data flow algorithm to perform the operation.
//
// For an expression tree, we walk the tree in post order, for each node
//
//  1) each node can be in 3 states, static, single, and mixed
//
//    1.1) static node means it always evalutes to value regardless of where it
//         is, ie a constant
//
//    1.2) single node means this node requires only current table to be
//         evaluated
//
//    1.3) mixed node means this node requires multiple tables or irrelevent
//         table to be evaluated, example t1.a + t2.b or t2.b when scan t1
//
//  Each node's state can be decided directly base on expression info
//
// The result of data flow algorithm is to decide each expression's filter
// status. A static status means the node can be inferred regardlessly, a related
// node means an expression can be evaluated directly based on current table and
// an unknown node means a node cannot be evaluated.
//
//  2) there's a meet operator defined as following
//
//   2.1) a binary node with operator AND,
//        As long as at least one of its 2 children is not unknown, then it is
//        not unknown. And it is static only if 2 chidlren are both static, this
//        means a constant folding opportunity though
//
//   2.2) Other node,
//        As long as at least one of its 2 children is unknown, then it is
//        unknown and it is static only if all of its children are static.
//
// ----------------------------------------------------------------------------

const (
	anaEFStatic = iota
	anaEFKnown
	anaEFUnknown
)

// Notes this function will return nil when it *cannot* perform early filter
func (self *Plan) anaEarlyFilter(
	tableIdx int, // assumed table index for current table scanning
	info *exprTableAccessInfo, // information about the input expression
	input sql.Expr, // input filter
) sql.Expr {

	a := &earlyFilterAnalyzer{
		p:      self,
		tidx:   tableIdx,
		info:   info,
		input:  input,
		output: make(map[sql.Expr]int),
	}
	return a.run()
}

func (self *Plan) isPrune(expr sql.Expr) bool {
	_, ok := self.prune[expr]
	return ok
}

func (self *Plan) unknownFilter(input sql.Expr) []sql.Expr {
	// DFS visiting the input expression until we hit following situation
	// 1) if current node, testify against isPrune returns true, ignore

	// 2) if current node, testify against isPrune returns false, if it is a
	//    not a binary node with logical operator AND, then just return the node
	//    otherwise, the node could be partially pruned, ie the parental node is
	//    not pruned, but its descendent may be pruned.

	// 1) if it is a pruned node, then just ignore it
	if self.isPrune(input) {
		return nil
	}

	// 2) it is not pruned
	if input.Type() == sql.ExprBinary {
		if bin := input.(*sql.Binary); bin.Op == sql.TkAnd {
			out := []sql.Expr{}
			out = append(out, self.unknownFilter(bin.L)...)
			out = append(out, self.unknownFilter(bin.R)...)
			return out
		}
	}

	// okay, we are sure this node cannot be partially pruned, then just return
	// it back
	return []sql.Expr{input}
}

type earlyFilterAnalyzer struct {
	p     *Plan
	tidx  int
	info  *exprTableAccessInfo
	input sql.Expr

	// output information
	output map[sql.Expr]int
}

func (self *earlyFilterAnalyzer) run() sql.Expr {
	self.anaExpr(self.input)
	expr := self.prune()
	self.resetCanName(expr)
	return expr
}

func (self *earlyFilterAnalyzer) doToStatus(expr sql.Expr) int {
	if set := self.info.setOrNil(expr); set == nil {
		return anaEFStatic
	} else {
		if set.Single() && set.hasTable(self.tidx) {
			return anaEFKnown
		} else if set.Static() {
			return anaEFStatic
		} else {
			return anaEFUnknown
		}
	}
}

func (self *earlyFilterAnalyzer) toStatus(expr sql.Expr) int {
	return self.doToStatus(expr)
}

// The algorithm is as following, since we can tell a node's type based on
// previous expression evaluation. We do a DFS of the tree in pre-order, for
// any node that is none binary+AND, we directly consult the previous expression
// phase result to testify whether this node is unknown or not, if so just do
// nothing but bailout. For node that is binary+AND, if there's one of its
// binary operands is unknown, we recursively visiting this node until we are
// done with above described situation. By this we sort of construct a partial
// tree which we can prune the tree as early filter
func (self *earlyFilterAnalyzer) anaExpr(
	expr sql.Expr,
) {
	switch expr.Type() {
	case sql.ExprBinary:
		binary := expr.(*sql.Binary)
		if binary.Op == sql.TkAnd {
			lhs := self.toStatus(binary.L)
			rhs := self.toStatus(binary.R)

			if lhs == anaEFStatic && rhs == anaEFStatic {
				self.output[binary] = anaEFStatic
			} else if lhs == anaEFUnknown && rhs == anaEFUnknown {
				self.anaExpr(binary.L)
				self.anaExpr(binary.R)
			} else {
				if lhs == anaEFUnknown {
					self.anaExpr(binary.L)
					self.output[binary.R] = rhs
				} else if rhs == anaEFUnknown {
					self.anaExpr(binary.R)
					self.output[binary.L] = lhs
				}
			}
		} else {
			self.ana(expr)
		}
		break
	default:
		self.ana(expr)
		break
	}
}

func (self *earlyFilterAnalyzer) ana(expr sql.Expr) {
	// no need to recursively vistiing, since no way to use them
	status := self.toStatus(expr)
	switch status {
	case anaEFKnown, anaEFStatic:
		self.output[expr] = status
		break
	default:
		break
	}
}

// the prune is relatively easy, all we need to do is to
func (self *earlyFilterAnalyzer) prune() sql.Expr {
	var cond sql.Expr

	// to make sure that each time the sql.Expr send out is stable in terms of
	// its order, associative order, we need to *sort* the self.output, notes
	// golang's map is unordered.

	exprList := []sql.Expr{}
	for n, _ := range self.output {
		exprList = append(exprList, n)
	}
	sort.Slice(
		exprList,
		func(i, j int) bool {
			// FIXME(dpeng): This is too inefficient, using address to do comparison
			return sql.PrintExpr(exprList[i]) < sql.PrintExpr(exprList[j])
		},
	)

	for _, n := range exprList {
		self.p.prune[n] = true // already pruned, can ignored later on

		if cond == nil {
			cond = sql.CloneExpr(n) // deep copy
		} else {
			newOne := &sql.Binary{
				Op: sql.TkAnd,
				L:  cond,
				R:  sql.CloneExpr(n),
			}
			cond = newOne
		}
	}

	return cond
}

type visitorEarlyFilterResetCanName struct {
}

func (self *visitorEarlyFilterResetCanName) setName(
	cn *sql.CanName,
	id string,
	cidx int,
) {
	switch cidx {
	case 0:
		cn.SetName("$0")
		break
	case ColumnIndexNF:
		cn.SetName("NF")
		break
	default:
		cn.SetName(id)
		break
	}
}

func (self *visitorEarlyFilterResetCanName) AcceptRef(
	ref *sql.Ref,
) (bool, error) {
	if ref.CanName.IsTableColumn() {
		self.setName(&ref.CanName, ref.Id, ref.CanName.ColumnIndex)
	} else {
		ref.CanName.SetName(ref.Id)
	}
	return true, nil
}

func (self *visitorEarlyFilterResetCanName) AcceptPrimary(
	primary *sql.Primary,
) (bool, error) {
	if primary.CanName.IsTableColumn() {
		self.setName(
			&primary.CanName,
			primary.Suffix[0].Component,
			primary.CanName.ColumnIndex,
		)
	}
	return true, nil
}

func (self *visitorEarlyFilterResetCanName) AcceptConst(
	*sql.Const,
) (bool, error) {
	return true, nil
}

func (self *visitorEarlyFilterResetCanName) AcceptSuffix(
	*sql.Suffix,
) (bool, error) {
	return true, nil
}

func (self *visitorEarlyFilterResetCanName) AcceptBinary(
	*sql.Binary,
) (bool, error) {
	return true, nil
}

func (self *visitorEarlyFilterResetCanName) AcceptTernary(
	*sql.Ternary,
) (bool, error) {
	return true, nil
}

func (self *visitorEarlyFilterResetCanName) AcceptUnary(
	*sql.Unary,
) (bool, error) {
	return true, nil
}

func (self *earlyFilterAnalyzer) resetCanName(
	x sql.Expr,
) {
	if x != nil {
		sql.VisitExprPreOrder(
			&visitorEarlyFilterResetCanName{},
			x,
		)
	}
}
