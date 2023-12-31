package cg

import (
	"fmt"
	"github.com/dianpeng/sql2awk/plan"
	"github.com/dianpeng/sql2awk/sql"
	"strings"
)

const aggTableIndex = -1

// expression generation
type exprCodeGen struct {
	cg *queryCodeGen
	o  strings.Builder
}

func (self *exprCodeGen) rid(idx int) string {
	return fmt.Sprintf("rid_%d", idx)
}

func (self *exprCodeGen) genConst(
	c *sql.Const,
) {
	switch c.Ty {
	case sql.ConstInt:
		self.o.WriteString(fmt.Sprintf("%d", c.Int))
		break
	case sql.ConstReal:
		self.o.WriteString(fmt.Sprintf("%f", c.Real))
		break
	case sql.ConstBool:
		if c.Bool {
			self.o.WriteString("1")
		} else {
			self.o.WriteString("0")
		}
		break
	case sql.ConstStr:
		self.o.WriteString(fmt.Sprintf("%q", c.String))
		break
	default:
		break
	}
}

func (self *exprCodeGen) genCanNameIndexOrRef(
	canName *sql.CanName,
) {
	switch canName.Type {
	case sql.CanNameName:
		self.o.WriteString(canName.Name)
		break

	case sql.CanNameTableColumn:
		if canName.TableIndex >= 0 {
			cidx := canName.ColumnIndex
			cidxStr := ""

			switch cidx {
			case plan.ColumnIndexNF:
				cidxStr = "\"$\""
				break

			case plan.ColumnIndexRowNum:
				cidxStr = "\"rownum\""
				break

			default:
				cidxStr = fmt.Sprintf("%d", cidx)
				break
			}
			self.o.WriteString(
				fmt.Sprintf(
					"%s[%s, %s]",
					self.cg.varTable(canName.TableIndex),
					self.rid(canName.TableIndex),
					cidxStr,
				),
			)
		} else if canName.TableIndex == aggTableIndex {
			self.o.WriteString(
				fmt.Sprintf(
					"%s[%d]",
					self.cg.varAggTable(),
					canName.ColumnIndex,
				),
			)
		} else {
			panic("unknown table")
		}
		break
	case sql.CanNameExpr:
		self.genSubExpr(canName.Reference)
		break
	default:
		break
	}
}

func (self *exprCodeGen) genRef(
	ref *sql.Ref,
) {
	if ref.CanName.IsFree() {
		// global variable, do nothing at all
		self.o.WriteString(ref.Id)
	} else {
		// based on the CanName to decide what to generate
		switch ref.CanName.Type {
		case sql.CanNameGlobal:
			self.o.WriteString(ref.Id)
			break
		case sql.CanNameTableColumn, sql.CanNameExpr, sql.CanNameName:
			self.genCanNameIndexOrRef(&ref.CanName)
			break
		default:
			panic("unreachable")
			break
		}
	}
}

func (self *exprCodeGen) functionName(
	primary *sql.Primary,
) string {
	if primary.Leading.Type() == sql.ExprRef &&
		len(primary.Suffix) >= 1 &&
		primary.Suffix[0].Ty == sql.SuffixCall {
		name := primary.Leading.(*sql.Ref)
		if name.CanName.IsFree() || name.CanName.IsName() {
			// for special functions, we do not translate into sql2awk_XXX but just
			// translate to builtin AWK builtins, since certain builtin allow any
			// number of parameters
			switch name.Id {
			case "string_format":
				return "sprintf"
			// bit operators
			case "bit_and":
				return "and"
			case "bit_or":
				return "or"
			case "bit_xor":
				return "xor"
			case "bit_not":
				return "compl"
			case "bit_lshift":
				return "lshift"
			case "bit_rshift":
				return "rshift"
			default:
				return fmt.Sprintf("sql2awk_%s", name.Id)
			}
		}
	}
	return ""
}

func (self *exprCodeGen) genPrimaryFree(
	primary *sql.Primary,
) {
	if n := self.functionName(primary); n != "" {
		self.o.WriteString(n)
	} else {
		self.genExpr(primary.Leading)
	}

	for _, x := range primary.Suffix {
		self.genSuffix(x)
	}
}

func (self *exprCodeGen) genPrimary(
	primary *sql.Primary,
) {
	if primary.CanName.IsFree() {
		self.genPrimaryFree(primary)
	} else {
		// based on the CanName to decide what to generate
		switch primary.CanName.Type {
		case sql.CanNameTableColumn, sql.CanNameName:
			self.genCanNameIndexOrRef(&primary.CanName)
			break
		default:
			panic("unreachable")
			break
		}
	}
}

func (self *exprCodeGen) genSuffix(
	suffix *sql.Suffix,
) {
	switch suffix.Ty {
	case sql.SuffixCall:
		self.o.WriteString("(")
		l := len(suffix.Call.Parameters)
		for idx, x := range suffix.Call.Parameters {
			self.genExpr(x)
			if idx < l-1 {
				self.o.WriteString(", ")
			}
		}
		self.o.WriteString(")")
		break

	case sql.SuffixIndex:
		self.o.WriteString("[")
		self.genExpr(suffix.Index)
		self.o.WriteString("]")
		break

	default:
		self.o.WriteString(".")
		self.o.WriteString(suffix.Component)
		break
	}
}

func (self *exprCodeGen) genUnary(
	unary *sql.Unary,
) {
	for _, x := range unary.Op {
		switch x {
		case sql.TkAdd:
			self.o.WriteString("+")
			break
		case sql.TkNot:
			self.o.WriteString("!")
			break
		case sql.TkSub:
			self.o.WriteString("-")
			break
		default:
			panic("unknown unary operator")
			break
		}
	}
	self.genSubExpr(unary.Operand)
}

func (self *exprCodeGen) genBinary(
	binary *sql.Binary,
) {
	self.o.WriteString("(")
	self.genExpr(binary.L)

	switch binary.Op {
	case sql.TkAdd:
		self.o.WriteString(" + ")
		break
	case sql.TkSub:
		self.o.WriteString(" - ")
		break
	case sql.TkMul:
		self.o.WriteString(" * ")
		break
	case sql.TkDiv:
		self.o.WriteString(" / ")
		break
	case sql.TkMod:
		self.o.WriteString(" % ")
		break
	case sql.TkAnd:
		self.o.WriteString(" && ")
		break
	case sql.TkOr:
		self.o.WriteString(" || ")
		break
	case sql.TkLt:
		self.o.WriteString(" < ")
		break
	case sql.TkLe:
		self.o.WriteString(" <= ")
		break
	case sql.TkGt:
		self.o.WriteString(" > ")
		break
	case sql.TkGe:
		self.o.WriteString(" >= ")
		break
	case sql.TkEq:
		self.o.WriteString(" == ")
		break
	case sql.TkNe:
		self.o.WriteString(" != ")
		break
	case sql.TkMatch:
		self.o.WriteString(" ~ ")
		break
	case sql.TkNotMatch:
		self.o.WriteString(" !~ ")
		break

	case sql.TkLike:
		self.o.WriteString(" ~ ")
		self.o.WriteString("like2r(")
		self.genExpr(binary.R)
		self.o.WriteString("))")
		return

	case sql.TkNotLike:
		self.o.WriteString(" !~ ")
		self.o.WriteString("like2r(")
		self.genExpr(binary.R)
		self.o.WriteString("))")
		return

	default:
		panic("unknown binary operator")
		break
	}

	self.genExpr(binary.R)
	self.o.WriteString(")")
}

func (self *exprCodeGen) genTernary(
	ternary *sql.Ternary,
) {
	self.o.WriteString("(")
	self.genSubExpr(ternary.Cond)
	self.o.WriteString("?")
	self.genSubExpr(ternary.B0)
	self.o.WriteString(":")
	self.genSubExpr(ternary.B1)
	self.o.WriteString(")")
}

func (self *exprCodeGen) genExpr(
	expr sql.Expr,
) {
	switch expr.Type() {
	case sql.ExprConst:
		self.genConst(expr.(*sql.Const))
		break
	case sql.ExprRef:
		self.genRef(expr.(*sql.Ref))
		break
	case sql.ExprSuffix:
		self.genSuffix(expr.(*sql.Suffix))
		break
	case sql.ExprPrimary:
		self.genPrimary(expr.(*sql.Primary))
		break
	case sql.ExprUnary:
		self.genUnary(expr.(*sql.Unary))
		break
	case sql.ExprBinary:
		self.genBinary(expr.(*sql.Binary))
		break
	case sql.ExprTernary:
		self.genTernary(expr.(*sql.Ternary))
		break
	default:
		panic("xxx: unknown expression")
		break
	}
}

func (self *exprCodeGen) genSubExpr(
	expr sql.Expr,
) {
	self.o.WriteString("(")
	self.genExpr(expr)
	self.o.WriteString(")")
}

func (self *exprCodeGen) genExprAsStr(
	expr sql.Expr,
) {
	self.o.WriteString("(")
	self.genSubExpr(expr)
	self.o.WriteString("\"\"")
	self.o.WriteString(")")
}
