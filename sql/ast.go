package sql

import (
	"bytes"
	"fmt"
	"strings"
)

const (
	ConstNull = iota
	ConstBool
	ConstStr
	ConstInt
	ConstReal
)

const (
	SuffixCall = iota
	SuffixDot
	SuffixIndex
)

const (
	ExprConst = iota
	ExprRef
	ExprSuffix
	ExprPrimary
	ExprUnary
	ExprBinary
	ExprTernary
)

const (
	SelectVarCol = iota
	SelectVarStar
)

const (
	OrderAsc = iota
	OrderDesc
)

type CodeInfo struct {
	Start   int
	End     int
	Snippet string
}

// Select statement, ie the only one we support for now :)

type SelectVar interface {
	Type() int
	CInfo() CodeInfo

	// Just an index that is unique within the SQL, used to represent the default
	// name when printing out. It is the index starting from 1, and can be used
	// reference back into the SelectVar array inside of the *From* clause
	Index() int

	// If the field has an aliased, via as keyword, then it returns otherwise
	// returns an empty string
	Alias() string

	// Column name is used to print the result out
	ColName() string
}

// Decorated expression, ie aggregation of expression, distinct of expression
// etc ...

type Col struct {
	CodeInfo CodeInfo
	ColIndex int
	As       string
	Value    Expr
	Rewrite  *Rewrite
}

type Star struct {
	CodeInfo CodeInfo
}

func (self *Col) Type() int       { return SelectVarCol }
func (self *Col) CInfo() CodeInfo { return self.CodeInfo }
func (self *Col) Index() int      { return self.ColIndex }
func (self *Col) Alias() string   { return self.As }
func (self *Col) ColName() string { return fmt.Sprintf("%d", self.ColIndex) }

func (self *Star) Type() int       { return SelectVarStar }
func (self *Star) CInfo() CodeInfo { return self.CodeInfo }
func (self *Star) Index() int      { return 0 }
func (self *Star) Alias() string   { return "" }
func (self *Star) ColName() string { return "%0" }

type SelectVarList []SelectVar

type Projection struct {
	CodeInfo  CodeInfo
	ValueList SelectVarList
}

func (self *SelectVarList) HasStar() bool {
	for _, y := range *self {
		if y.Type() == SelectVarStar {
			return true
		}
	}
	return false
}

func (self *Projection) HasStar() bool {
	return self.ValueList.HasStar()
}

type ColName struct {
	Table   string
	Name    string
	CanName CanName
}

// From is a format of *function call* here, but just allow constant arguments
type FromVar struct {
	Vars    []*Const
	Rewrite *Rewrite
	Name    string
	Alias   string // name of the table, ie aliased etc ...
}

type RewriteSet struct {
	Column string
	Value  Expr
}

type RewriteClause struct {
	CodeInfo CodeInfo
	When     Expr          // Condition of rewrite operation
	Set      []*RewriteSet // list of set
}

func (self *RewriteClause) Ignore() bool { return len(self.Set) == 0 }

type Rewrite struct {
	CodeInfo CodeInfo
	List     []*RewriteClause
}

type From struct {
	CodeInfo CodeInfo
	VarList  []*FromVar
}

// Where clause, just a list of expressions
type Where struct {
	CodeInfo  CodeInfo
	Condition Expr
}

type Having Where

type GroupBy struct {
	CodeInfo CodeInfo
	Name     []Expr
}

type OrderBy struct {
	CodeInfo CodeInfo
	Order    int
	Name     []Expr
}

type Limit struct {
	CodeInfo CodeInfo
	Limit    int64
}

// Extension to original SQL, allow user to dump the table in a better way
// For format, we allow known value's toggles to be set

type FormatColumn struct {
	Index int
	Value *Const
}

type Format struct {
	Title   *Const // title of the table
	Border  *Const // border of thet able
	Base    *Const // base policy of the format
	Number  *Const
	String  *Const
	Rest    *Const
	Padding *Const         // padding size
	Column  []FormatColumn // column customization
}

type Select struct {
	CodeInfo CodeInfo
	Distinct bool // whether a distinct selection, ie dedup

	Projection *Projection // projection
	From       *From       // from clause
	Where      *Where      // where clause
	GroupBy    *GroupBy    // group by
	Having     *Having     // having
	OrderBy    *OrderBy    // order by
	Limit      *Limit      // limit clause
	Format     *Format     // format of the select, when dumpped
}

type Code struct {
	CodeInfo CodeInfo
	Select   *Select
}

/** -------------------------------------------------------------------------
 ** Expression
 ** -----------------------------------------------------------------------*/
type Const struct {
	Ty       int
	Bool     bool
	String   string
	Real     float64
	Int      int64
	CodeInfo CodeInfo
}

type ConstList []*Const

const (
	SymbolNone = iota
	SymbolStar
	SymbolColumns
	SymbolRows
)

type Ref struct {
	Id       string
	CodeInfo CodeInfo
	CanName  CanName
	Symbol   int
}

type Call struct {
	Parameters []Expr
	CodeInfo   CodeInfo
}

type Suffix struct {
	Ty        int
	Call      *Call
	Index     Expr
	Component string
	Symbol    int // used along with component, when Ty is Dot, to indicate internal symbol
	CodeInfo  CodeInfo
}

type Primary struct {
	Leading  Expr
	Suffix   []*Suffix
	CodeInfo CodeInfo
	CanName  CanName
}

type Unary struct {
	Op       []int
	Operand  Expr
	CodeInfo CodeInfo
}

type Binary struct {
	Op       int
	L        Expr
	R        Expr
	CodeInfo CodeInfo
}

type Ternary struct {
	Cond     Expr
	B0       Expr
	B1       Expr
	CodeInfo CodeInfo
}

type Expr interface {
	Type() int
	CInfo() CodeInfo
}

func (self *Const) Type() int       { return ExprConst }
func (self *Const) CInfo() CodeInfo { return self.CodeInfo }

func (self *Ref) Type() int       { return ExprRef }
func (self *Ref) CInfo() CodeInfo { return self.CodeInfo }

func (self *Suffix) Type() int       { return ExprSuffix }
func (self *Suffix) CInfo() CodeInfo { return self.CodeInfo }

func (self *Primary) Type() int       { return ExprPrimary }
func (self *Primary) CInfo() CodeInfo { return self.CodeInfo }

func (self *Unary) Type() int       { return ExprUnary }
func (self *Unary) CInfo() CodeInfo { return self.CodeInfo }

func (self *Binary) Type() int       { return ExprBinary }
func (self *Binary) CInfo() CodeInfo { return self.CodeInfo }

func (self *Ternary) Type() int       { return ExprTernary }
func (self *Ternary) CInfo() CodeInfo { return self.CodeInfo }

func (self *ConstList) AsInt(idx int, def int) int {
	if idx >= len(*self) {
		return def
	}
	c := (*self)[idx]
	if c.Ty != ConstInt {
		return def
	}
	return int(c.Int)
}

func (self *ConstList) AsStr(idx int, def string) string {
	if idx >= len(*self) {
		return def
	}
	c := (*self)[idx]
	if c.Ty != ConstStr {
		return def
	}
	return c.String
}

func (self *ConstList) AsBool(idx int, def bool) bool {
	if idx >= len(*self) {
		return def
	}
	c := (*self)[idx]
	if c.Ty != ConstBool {
		return def
	}
	return c.Bool
}

func (self *ConstList) AsReal(idx int, def float64) float64 {
	if idx >= len(*self) {
		return def
	}
	c := (*self)[idx]
	if c.Ty != ConstReal {
		return def
	}
	return c.Real
}

/* ----------------------------------------------------------------------------
 * Visitor
 * ---------------------------------------------------------------------------*/

type ExprVisitor interface {
	AcceptConst(*Const) (bool, error)
	AcceptRef(*Ref) (bool, error)
	AcceptSuffix(*Suffix) (bool, error)
	AcceptPrimary(*Primary) (bool, error)
	AcceptUnary(*Unary) (bool, error)
	AcceptBinary(*Binary) (bool, error)
	AcceptTernary(*Ternary) (bool, error)
}

func visitExprPostOrder(
	visitor ExprVisitor,
	expr Expr,
) error {
	switch expr.Type() {
	case ExprConst:
		if _, err := visitor.AcceptConst(expr.(*Const)); err != nil {
			return err
		}
		return nil

	case ExprRef:
		if _, err := visitor.AcceptRef(expr.(*Ref)); err != nil {
			return err
		}
		return nil

	case ExprSuffix:
		suff := expr.(*Suffix)
		switch suff.Ty {
		case SuffixCall:
			for _, x := range suff.Call.Parameters {
				if err := visitExprPostOrder(visitor, x); err != nil {
					return err
				}
			}
			break
		case SuffixIndex:
			return visitExprPostOrder(visitor, suff.Index)
		default:
			break
		}
		if _, err := visitor.AcceptSuffix(suff); err != nil {
			return err
		}
		return nil
	case ExprPrimary:
		primary := expr.(*Primary)
		if err := visitExprPostOrder(visitor, primary.Leading); err != nil {
			return err
		}
		for _, x := range primary.Suffix {
			if err := visitExprPostOrder(visitor, x); err != nil {
				return err
			}
		}
		if _, err := visitor.AcceptPrimary(primary); err != nil {
			return err
		}
		return nil

	case ExprUnary:
		unary := expr.(*Unary)
		if err := visitExprPostOrder(visitor, unary.Operand); err != nil {
			return err
		}
		if _, err := visitor.AcceptUnary(unary); err != nil {
			return err
		}
		return nil

	case ExprBinary:
		binary := expr.(*Binary)
		if err := visitExprPostOrder(visitor, binary.L); err != nil {
			return err
		}
		if err := visitExprPostOrder(visitor, binary.R); err != nil {
			return err
		}
		if _, err := visitor.AcceptBinary(binary); err != nil {
			return err
		}
		return nil

	case ExprTernary:
		ternary := expr.(*Ternary)
		if err := visitExprPostOrder(visitor, ternary.Cond); err != nil {
			return err
		}
		if err := visitExprPostOrder(visitor, ternary.B0); err != nil {
			return err
		}
		if err := visitExprPostOrder(visitor, ternary.B1); err != nil {
			return err
		}
		if _, err := visitor.AcceptTernary(ternary); err != nil {
			return err
		}
		return nil
	default:
		return nil
	}
}

func visitExprPreOrder(
	visitor ExprVisitor,
	expr Expr,
) error {
	switch expr.Type() {
	case ExprConst:
		if _, err := visitor.AcceptConst(expr.(*Const)); err != nil {
			return err
		}
		return nil

	case ExprRef:
		if _, err := visitor.AcceptRef(expr.(*Ref)); err != nil {
			return err
		}
		return nil

	case ExprSuffix:
		suff := expr.(*Suffix)
		if goon, err := visitor.AcceptSuffix(suff); err != nil {
			return err
		} else if err != nil {
			return err
		} else if goon {
			switch suff.Ty {
			case SuffixCall:
				for _, x := range suff.Call.Parameters {
					if err := visitExprPreOrder(visitor, x); err != nil {
						return err
					}
				}
				break
			case SuffixIndex:
				return visitExprPreOrder(visitor, suff.Index)
			default:
				break
			}
		}
		return nil
	case ExprPrimary:
		primary := expr.(*Primary)
		if goon, err := visitor.AcceptPrimary(primary); err != nil {
			return err
		} else if err != nil {
			return err
		} else if goon {
			if err := visitExprPreOrder(visitor, primary.Leading); err != nil {
				return err
			}
			for _, x := range primary.Suffix {
				if err := visitExprPreOrder(visitor, x); err != nil {
					return err
				}
			}
		}
		return nil

	case ExprUnary:
		unary := expr.(*Unary)
		if goon, err := visitor.AcceptUnary(unary); err != nil {
			return err
		} else if goon {
			return visitExprPreOrder(visitor, unary.Operand)
		}
		return nil

	case ExprBinary:
		binary := expr.(*Binary)
		if goon, err := visitor.AcceptBinary(binary); err != nil {
			return err
		} else if goon {
			if err := visitExprPreOrder(visitor, binary.L); err != nil {
				return err
			}
			if err := visitExprPreOrder(visitor, binary.R); err != nil {
				return err
			}
		}
		return nil

	case ExprTernary:
		ternary := expr.(*Ternary)
		if goon, err := visitor.AcceptTernary(ternary); err != nil {
			return err
		} else if goon {
			if err := visitExprPreOrder(visitor, ternary.Cond); err != nil {
				return err
			}
			if err := visitExprPreOrder(visitor, ternary.B0); err != nil {
				return err
			}
			if err := visitExprPreOrder(visitor, ternary.B1); err != nil {
				return err
			}
		}
		return nil
	default:
		return nil
	}
}

func VisitExprPreOrder(
	visitor ExprVisitor,
	expr Expr,
) error {
	return visitExprPreOrder(visitor, expr)
}

func VisitExprPostOrder(
	visitor ExprVisitor,
	expr Expr,
) error {
	return visitExprPostOrder(visitor, expr)
}

/* ----------------------------------------------------------------------------
 * Clone
 * ---------------------------------------------------------------------------*/
func cloneExprConst(
	in *Const,
) *Const {
	value := *in
	return &value
}

func cloneExprRef(
	in *Ref,
) *Ref {
	value := *in
	return &value
}

func cloneExprCall(
	in *Call,
) *Call {
	if in == nil {
		return nil
	}
	c := &Call{
		CodeInfo: in.CodeInfo,
	}
	for _, x := range in.Parameters {
		c.Parameters = append(c.Parameters, cloneExpr(x))
	}
	return c
}

func cloneExprSuffix(
	in *Suffix,
) *Suffix {
	return &Suffix{
		Ty:        in.Ty,
		Call:      cloneExprCall(in.Call),
		Index:     cloneExpr(in.Index),
		Component: in.Component,
		CodeInfo:  in.CodeInfo,
	}
}

func cloneExprPrimary(
	in *Primary,
) *Primary {
	p := &Primary{
		Leading:  cloneExpr(in.Leading),
		CodeInfo: in.CodeInfo,
		CanName:  in.CanName,
	}
	for _, x := range in.Suffix {
		p.Suffix = append(p.Suffix, cloneExprSuffix(x))
	}
	return p
}

func cloneExprUnary(
	in *Unary,
) *Unary {
	return &Unary{
		Op:       in.Op,
		Operand:  cloneExpr(in.Operand),
		CodeInfo: in.CodeInfo,
	}
}

func cloneExprBinary(
	in *Binary,
) *Binary {
	return &Binary{
		Op:       in.Op,
		L:        cloneExpr(in.L),
		R:        cloneExpr(in.R),
		CodeInfo: in.CodeInfo,
	}
}

func cloneExprTernary(
	in *Ternary,
) *Ternary {
	return &Ternary{
		Cond:     cloneExpr(in.Cond),
		B0:       cloneExpr(in.B0),
		B1:       cloneExpr(in.B1),
		CodeInfo: in.CodeInfo,
	}
}

func cloneExpr(
	in Expr,
) Expr {
	if in == nil {
		return nil
	}
	switch in.Type() {
	case ExprConst:
		return cloneExprConst(in.(*Const))
	case ExprRef:
		return cloneExprRef(in.(*Ref))
	case ExprSuffix:
		return cloneExprSuffix(in.(*Suffix))
	case ExprPrimary:
		return cloneExprPrimary(in.(*Primary))
	case ExprUnary:
		return cloneExprUnary(in.(*Unary))
	case ExprBinary:
		return cloneExprBinary(in.(*Binary))
	case ExprTernary:
		return cloneExprTernary(in.(*Ternary))
	default:
		return nil
	}
}

func CloneExpr(in Expr) Expr {
	return cloneExpr(in)
}

/* ----------------------------------------------------------------------------
 * Printing
 * ---------------------------------------------------------------------------*/

// Stringify the AST. We do not use method but use free function
func indent(sz int) string {
	return strings.Repeat("  ", sz)
}

func indentS(sz int, msg string) string {
	return fmt.Sprintf("%s%s", indent(sz), msg)
}

func doPrintExprConst(c *Const, buf *bytes.Buffer, _ int) {
	switch c.Ty {
	case ConstBool:
		buf.WriteString(fmt.Sprintf("%t", c.Bool))
		break
	case ConstStr:
		buf.WriteString(fmt.Sprintf("%q", c.String))
		break
	case ConstInt:
		buf.WriteString(fmt.Sprintf("%d", c.Int))
		break
	case ConstReal:
		buf.WriteString(fmt.Sprintf("%f", c.Real))
		break
	case ConstNull:
		buf.WriteString("null")
		break
	default:
		panic("unreachable")
		break
	}
}

func doPrintExprRef(r *Ref, buf *bytes.Buffer, _ int) {
	buf.WriteString(r.Id)
}

func doPrintExprSuffix(s *Suffix, buf *bytes.Buffer, ind int) {
	switch s.Ty {
	case SuffixCall:
		// printing calls
		buf.WriteString("(")
		idx := 0
		sz := len(s.Call.Parameters)

		for _, entry := range s.Call.Parameters {
			doPrintExpr(entry, buf, ind)
			if idx < sz-1 {
				buf.WriteString(",")
			}
			idx++
		}
		buf.WriteString(")")
		break

	case SuffixDot:
		buf.WriteString(".")
		buf.WriteString(fmt.Sprintf("%q", s.Component))
		break

	case SuffixIndex:
		buf.WriteString("[")
		doPrintExpr(s.Index, buf, ind)
		buf.WriteString("]")
		break

	default:
		panic("unreachable")
		break
	}
}

func doPrintExprPrimary(p *Primary, buf *bytes.Buffer, ind int) {
	doPrintExpr(p.Leading, buf, ind)
	for _, entry := range p.Suffix {
		doPrintExprSuffix(entry, buf, ind)
	}
}

func doPrintExprUnary(u *Unary, buf *bytes.Buffer, ind int) {
	for _, o := range u.Op {
		switch o {
		case TkAdd:
			buf.WriteString("+")
			break
		case TkSub:
			buf.WriteString("-")
			break
		case TkNot:
			buf.WriteString("!")
			break
		default:
			panic("unreachable")
			break
		}
	}
	doPrintExpr(u.Operand, buf, ind)
}

func doPrintExprBinary(b *Binary, buf *bytes.Buffer, ind int) {
	buf.WriteString("(")
	doPrintExpr(b.L, buf, ind)
	switch b.Op {
	case TkAdd:
		buf.WriteString("+")
		break
	case TkSub:
		buf.WriteString("-")
		break
	case TkMul:
		buf.WriteString("*")
		break
	case TkDiv:
		buf.WriteString("/")
		break
	case TkMod:
		buf.WriteString("%")
		break
	case TkLt:
		buf.WriteString("<")
		break
	case TkLe:
		buf.WriteString("<=")
		break
	case TkGt:
		buf.WriteString(">")
		break
	case TkGe:
		buf.WriteString(">=")
		break
	case TkEq:
		buf.WriteString("==")
		break
	case TkNe:
		buf.WriteString("!=")
		break
	case TkAnd:
		buf.WriteString("&&")
		break
	case TkOr:
		buf.WriteString("||")
		break
	case TkMatch:
		buf.WriteString(" match ")
		break
	case TkNotMatch:
		buf.WriteString(" not match ")
		break
	case TkLike:
		buf.WriteString(" like ")
		break
	case TkNotLike:
		buf.WriteString(" not like ")
		break
	case TkIn:
		buf.WriteString(" in ")
		break
	case TkBetween:
		buf.WriteString(" between ")
		break
	case tkNotIn:
		buf.WriteString(" not in ")
		break
	case tkNotBetween:
		buf.WriteString(" not between ")
		break
	default:
		panic("unreachable")
		break
	}
	doPrintExpr(b.R, buf, ind)
	buf.WriteString(")")
}

func doPrintExprTernary(t *Ternary, buf *bytes.Buffer, ind int) {
	doPrintExpr(t.Cond, buf, ind)
	buf.WriteString(" ? ")
	doPrintExpr(t.B0, buf, ind)
	buf.WriteString(" : ")
	doPrintExpr(t.B1, buf, ind)
}

func doPrintExpr(expr Expr, buf *bytes.Buffer, ind int) {
	switch expr.Type() {
	case ExprConst:
		c := expr.(*Const)
		doPrintExprConst(c, buf, ind)
		break

	case ExprRef:
		r := expr.(*Ref)
		doPrintExprRef(r, buf, ind)
		break

	case ExprPrimary:
		s := expr.(*Primary)
		doPrintExprPrimary(s, buf, ind)
		break

	case ExprUnary:
		u := expr.(*Unary)
		doPrintExprUnary(u, buf, ind)
		break

	case ExprBinary:
		b := expr.(*Binary)
		doPrintExprBinary(b, buf, ind)
		break

	case ExprTernary:
		t := expr.(*Ternary)
		doPrintExprTernary(t, buf, ind)
		break

	case ExprSuffix:
		t := expr.(*Suffix)
		doPrintExprSuffix(t, buf, ind)
		break

	default:
		panic("unreachable")
		break
	}
}

// ----------------------------------------------------------------------------
// Statement
// ----------------------------------------------------------------------------
func doPrintStmtProjection(projection *Projection, buf *bytes.Buffer, ind int) {
	l := len(projection.ValueList)

	for idx, x := range projection.ValueList {
		switch x.Type() {
		case SelectVarCol:
			col := x.(*Col)

			// print the expression itself
			doPrintExpr(col.Value, buf, ind)

			// print alias if any
			if col.As != "" {
				buf.WriteString(" as ")
				buf.WriteString(col.As)
			}
			break

		default:
			buf.WriteString("*")
			break
		}

		if idx < l-1 {
			buf.WriteString(", ")
		}
	}
}

func doPrintStmtFrom(from *From, buf *bytes.Buffer, ind int) {
	buf.WriteString("\nfrom ")
	l := len(from.VarList)

	for idx, x := range from.VarList {
		buf.WriteString(x.Name)
		buf.WriteString("(")
		ll := len(x.Vars)

		for iidx, y := range x.Vars {
			doPrintExprConst(y, buf, ind)
			if iidx < ll-1 {
				buf.WriteString(", ")
			}
		}
		buf.WriteString(")")
		if x.Alias != "" {
			buf.WriteString(" as ")
			buf.WriteString(x.Alias)
		}

		if idx < l-1 {
			buf.WriteString(", ")
		}
	}
}

func doPrintStmtWhere(where *Where, buf *bytes.Buffer, ind int) {
	buf.WriteString("\nwhere ")
	doPrintExpr(where.Condition, buf, ind)
}

func doPrintStmtGroupBy(gb *GroupBy, buf *bytes.Buffer, ind int) {
	buf.WriteString("\ngroup by ")
	l := len(gb.Name)
	for idx, x := range gb.Name {
		doPrintExpr(x, buf, ind)
		if idx < l-1 {
			buf.WriteString(", ")
		}
	}
}

func doPrintStmtHaving(having *Having, buf *bytes.Buffer, ind int) {
	buf.WriteString("\nhaving ")
	doPrintExpr(having.Condition, buf, ind)
}

func doPrintStmtOrderBy(orderBy *OrderBy, buf *bytes.Buffer, ind int) {
	buf.WriteString("\norder by ")

	l := len(orderBy.Name)

	for idx, x := range orderBy.Name {
		doPrintExpr(x, buf, ind)
		if idx < l-1 {
			buf.WriteString(", ")
		}
	}

	if orderBy.Order == OrderAsc {
		buf.WriteString(" asc")
	} else {
		buf.WriteString(" desc")
	}
}

func doPrintStmtLimit(limit *Limit, buf *bytes.Buffer, ind int) {
	buf.WriteString("\nlimit ")
	buf.WriteString(fmt.Sprintf("%d", limit.Limit))
}

func doPrintSelect(s *Select, buf *bytes.Buffer, ind int) {
	if s.Distinct {
		buf.WriteString("select distinct\n")
	} else {
		buf.WriteString("select\n")
	}

	doPrintStmtProjection(s.Projection, buf, ind)
	doPrintStmtFrom(s.From, buf, ind)

	if s.Where != nil {
		doPrintStmtWhere(s.Where, buf, ind)
	}
	if s.GroupBy != nil {
		doPrintStmtGroupBy(s.GroupBy, buf, ind)
	}
	if s.Having != nil {
		doPrintStmtHaving(s.Having, buf, ind)
	}
	if s.OrderBy != nil {
		doPrintStmtOrderBy(s.OrderBy, buf, ind)
	}
	if s.Limit != nil {
		doPrintStmtLimit(s.Limit, buf, ind)
	}
}

func PrintExpr(expr Expr) string {
	if expr == nil {
		return ""
	}
	b := &bytes.Buffer{}
	doPrintExpr(expr, b, 0)
	return b.String()
}

func PrintSelect(s *Select) string {
	b := &bytes.Buffer{}
	doPrintSelect(s, b, 0)
	return b.String()
}

func PrintExprWithIndent(expr Expr, ind int) string {
	b := &bytes.Buffer{}
	doPrintExpr(expr, b, ind)
	return b.String()
}

func PrintCode(c *Code) string {
	b := &bytes.Buffer{}
	doPrintSelect(c.Select, b, 0)
	return b.String()
}
