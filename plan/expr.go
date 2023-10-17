package plan

import (
	"fmt"
	"github.com/dianpeng/sql2awk/sql"
	"strings"
)

// Expression phase is a simple data flow algorithm pass which walk through the
// expression AST and mark each *node* inside of the tree along with its property
//
// 1) For each expression node, regardless it is a leaf or internal node, assign
//    a *set* to this node.
//
// 2) Walk the tree in a data flow fashion, post order, and set the set to be
//    *include* of its children's node's set
//
// 3) For node that contains a *CanName* object, ie the node sql.Primary and
//    sql.Ref, if it is a *table column*, testified via CanName.IsTableColumn
//    then add TableIndex into its set
//
// Finally we learn that each node's table access set.

// the set object used to track all the table index belongs to a certain expr
type exprTableAccessSet map[int]bool

func (self *exprTableAccessSet) Has(tidx int) bool {
	_, ok := (*self)[tidx]
	return ok
}

func (self *exprTableAccessSet) Static() bool {
	return len(*self) == 0
}

func (self *exprTableAccessSet) Single() bool {
	return len(*self) == 1
}

func (self *exprTableAccessSet) Print() string {
	buf := strings.Builder{}
	for k, _ := range *self {
		buf.WriteString(fmt.Sprintf("%d;", k))
	}
	return buf.String()
}

type exprTableAccessInfo struct {
	root sql.Expr                        // root expression
	info map[sql.Expr]exprTableAccessSet // expression -> set mapping
}

func newExprTableAccessInfo(root sql.Expr) *exprTableAccessInfo {
	info := &exprTableAccessInfo{
		root: root,
		info: make(map[sql.Expr]exprTableAccessSet),
	}
	info.mark(root)
	return info
}

func (self *exprTableAccessInfo) s(expr sql.Expr) exprTableAccessSet {
	r, ok := self.info[expr]
	if !ok {
		r = make(exprTableAccessSet)
		self.info[expr] = r
	}
	return r
}

func (self *exprTableAccessInfo) setOrNil(expr sql.Expr) exprTableAccessSet {
	r, _ := self.info[expr]
	return r
}

func (self *exprTableAccessInfo) Ref(expr sql.Expr, tidx int) bool {
	s := self.s(expr)

	return s.Has(tidx)
}

func (self *exprTableAccessInfo) Print() string {
	buf := strings.Builder{}
	for key, value := range self.info {
		buf.WriteString(
			fmt.Sprintf("expr{%s} => {%s}\n", sql.PrintExpr(key), value.Print()),
		)
	}
	return buf.String()
}

func (self *exprTableAccessInfo) include(
	dst exprTableAccessSet,
	src exprTableAccessSet,
) {
	for k, _ := range src {
		dst[k] = true
	}
}

// make sure that each expression has already been settled, otherwise it panic
func (self *exprTableAccessInfo) mark(
	expr sql.Expr,
) {
	// The visiting order is *post order*, ie first its children and then all the
	// rest
	switch expr.Type() {
	default:
		break

	case sql.ExprRef:
		self.markRef(expr.(*sql.Ref))
		break

	case sql.ExprPrimary:
		self.markPrimary(expr.(*sql.Primary))
		break

	case sql.ExprSuffix:
		self.markSuffix(expr.(*sql.Suffix))
		break

	case sql.ExprUnary:
		self.markUnary(expr.(*sql.Unary))
		break

	case sql.ExprBinary:
		self.markBinary(expr.(*sql.Binary))
		break

	case sql.ExprTernary:
		self.markTernary(expr.(*sql.Ternary))
		break
	}
}

func (self *exprTableAccessInfo) markRef(
	ref *sql.Ref,
) {
	if ref.CanName.IsTableColumn() {
		set := self.s(ref)
		set[ref.CanName.TableIndex] = true
	}
}

func (self *exprTableAccessInfo) markSuffixCall(
	suffix *sql.Suffix,
) {
	for _, x := range suffix.Call.Parameters {
		self.mark(x)
	}

	set := self.s(suffix)

	// include all its children's set
	for _, x := range suffix.Call.Parameters {
		thatSet := self.s(x)
		self.include(set, thatSet)
	}
}

func (self *exprTableAccessInfo) markSuffixIndex(
	suffix *sql.Suffix,
) {
	self.mark(suffix.Index)
	self.include(self.s(suffix), self.s(suffix.Index))
}

func (self *exprTableAccessInfo) markSuffix(
	suffix *sql.Suffix,
) {
	switch suffix.Ty {
	case sql.SuffixCall:
		self.markSuffixCall(suffix)
		break

	case sql.SuffixIndex:
		self.markSuffixIndex(suffix)
		break

	default:
		break
	}
}

func (self *exprTableAccessInfo) markPrimary(
	primary *sql.Primary,
) {
	if primary.CanName.IsTableColumn() {
		set := self.s(primary)
		set[primary.CanName.TableIndex] = true
	} else {
		self.mark(primary.Leading)
		for _, x := range primary.Suffix {
			self.markSuffix(x)
		}

		set := self.s(primary)

		self.include(set, self.s(primary.Leading))
		for _, x := range primary.Suffix {
			self.include(set, self.s(x))
		}
	}
}

func (self *exprTableAccessInfo) markUnary(
	unary *sql.Unary,
) {
	self.mark(unary.Operand)
	self.include(self.s(unary), self.s(unary.Operand))
}

func (self *exprTableAccessInfo) markBinary(
	binary *sql.Binary,
) {
	self.mark(binary.L)
	self.mark(binary.R)
	set := self.s(binary)
	self.include(set, self.s(binary.L))
	self.include(set, self.s(binary.R))
}

func (self *exprTableAccessInfo) markTernary(
	ternary *sql.Ternary,
) {
	self.mark(ternary.Cond)
	self.mark(ternary.B0)
	self.mark(ternary.B1)

	set := self.s(ternary)

	self.include(set, self.s(ternary.Cond))
	self.include(set, self.s(ternary.B0))
	self.include(set, self.s(ternary.B1))
}
