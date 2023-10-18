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
type exprTableAccessSet struct {
	set map[int][]int
}

func (self *exprTableAccessSet) length() int {
	return len(self.set)
}

func (self *exprTableAccessSet) hasTable(tidx int) bool {
	v, _ := self.set[tidx]
	return len(v) > 0
}

func (self *exprTableAccessSet) has(
	tidx int,
	fidx int,
) bool {
	if x, ok := self.set[tidx]; ok {
		for _, v := range x {
			if v == fidx {
				return true
			}
		}
	}
	return false
}

func (self *exprTableAccessSet) Static() bool {
	return len(self.set) == 0
}

func (self *exprTableAccessSet) Single() bool {
	return len(self.set) == 1
}

func (self *exprTableAccessSet) Print() string {
	buf := strings.Builder{}
	for tidx, tset := range self.set {
		for _, fidx := range tset {
			buf.WriteString(fmt.Sprintf("[%d:%d];", tidx, fidx))
		}
	}
	return buf.String()
}

func (self *exprTableAccessSet) add(
	tidx int,
	fidx int,
) {
	v, ok := self.set[tidx]
	if ok {
		v = append(v, fidx)
	} else {
		v = []int{fidx}
	}
	self.set[tidx] = v
}

func (self *exprTableAccessSet) unionFidxArray(
	dst []int,
	src []int,
) []int {
	tmp := append(dst, src...)
	seen := make(map[int]bool)
	out := []int{}
	for _, x := range tmp {
		if _, ok := seen[x]; !ok {
			seen[x] = true
			out = append(out, x)
		}
	}
	return out
}

func (self *exprTableAccessSet) containFidxArray(
	lhs []int,
	rhs []int,
) bool {
	for _, vv := range rhs {
		found := false
		for _, vvv := range lhs {
			if vvv == vv {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func (self *exprTableAccessSet) union(
	that *exprTableAccessSet,
) {
	for tidx, rhs := range that.set {
		v, _ := self.set[tidx]
		v = self.unionFidxArray(
			v,
			rhs,
		)
		self.set[tidx] = v
	}
}

func (self *exprTableAccessSet) contain(
	that *exprTableAccessSet,
) bool {
	for tidx, rhs := range that.set {
		v, _ := self.set[tidx]
		if !self.containFidxArray(v, rhs) {
			return false
		}
	}
	return true
}

func newExprTableAccessSet() *exprTableAccessSet {
	return &exprTableAccessSet{
		set: make(map[int][]int),
	}
}

type exprTableAccessInfo struct {
	root sql.Expr                         // root expression
	info map[sql.Expr]*exprTableAccessSet // expression -> set mapping
}

func newExprTableAccessInfo(root sql.Expr) *exprTableAccessInfo {
	info := &exprTableAccessInfo{
		root: root,
		info: make(map[sql.Expr]*exprTableAccessSet),
	}
	info.mark(root)
	return info
}

func getExprTableAccessSet(x sql.Expr) *exprTableAccessSet {
	info := newExprTableAccessInfo(x)
	return info.s(x)
}

func (self *exprTableAccessInfo) s(expr sql.Expr) *exprTableAccessSet {
	r, ok := self.info[expr]
	if !ok {
		r = newExprTableAccessSet()
		self.info[expr] = r
	}
	return r
}

func (self *exprTableAccessInfo) setOrNil(expr sql.Expr) *exprTableAccessSet {
	r, _ := self.info[expr]
	return r
}

func (self *exprTableAccessInfo) Ref(expr sql.Expr, tidx int) bool {
	s := self.s(expr)

	return s.hasTable(tidx)
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
	dst *exprTableAccessSet,
	src *exprTableAccessSet,
) {
	dst.union(src)
}

// make sure that each expression has already been settled, otherwise it panic
func (self *exprTableAccessInfo) mark(
	expr sql.Expr,
) {
	sql.VisitExprPostOrder(
		self,
		expr,
	)
}

func (self *exprTableAccessInfo) AcceptConst(
	*sql.Const,
) (bool, error) {
	return true, nil
}

func (self *exprTableAccessInfo) AcceptRef(
	ref *sql.Ref,
) (bool, error) {
	if ref.CanName.IsTableColumn() {
		set := self.s(ref)
		set.add(
			ref.CanName.TableIndex,
			ref.CanName.ColumnIndex,
		)
	}
	return true, nil
}

func (self *exprTableAccessInfo) markSuffixCall(
	suffix *sql.Suffix,
) {
	set := self.s(suffix)
	for _, x := range suffix.Call.Parameters {
		thatSet := self.s(x)
		self.include(set, thatSet)
	}
}

func (self *exprTableAccessInfo) markSuffixIndex(
	suffix *sql.Suffix,
) {
	self.include(self.s(suffix), self.s(suffix.Index))
}

func (self *exprTableAccessInfo) AcceptSuffix(
	suffix *sql.Suffix,
) (bool, error) {
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
	return true, nil
}

func (self *exprTableAccessInfo) AcceptPrimary(
	primary *sql.Primary,
) (bool, error) {
	if primary.CanName.IsTableColumn() {
		set := self.s(primary)
		set.add(
			primary.CanName.TableIndex,
			primary.CanName.ColumnIndex,
		)
	} else {
		set := self.s(primary)
		self.include(set, self.s(primary.Leading))
		for _, x := range primary.Suffix {
			self.include(set, self.s(x))
		}
	}
	return true, nil
}

func (self *exprTableAccessInfo) AcceptUnary(
	unary *sql.Unary,
) (bool, error) {
	self.include(self.s(unary), self.s(unary.Operand))
	return true, nil
}

func (self *exprTableAccessInfo) AcceptBinary(
	binary *sql.Binary,
) (bool, error) {
	set := self.s(binary)
	self.include(set, self.s(binary.L))
	self.include(set, self.s(binary.R))
	return true, nil
}

func (self *exprTableAccessInfo) AcceptTernary(
	ternary *sql.Ternary,
) (bool, error) {
	set := self.s(ternary)

	self.include(set, self.s(ternary.Cond))
	self.include(set, self.s(ternary.B0))
	self.include(set, self.s(ternary.B1))
	return true, nil
}
