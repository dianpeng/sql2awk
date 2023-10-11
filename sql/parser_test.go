package sql

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func doTestSelect(lhs, rhs string, assert *assert.Assertions) {
	p := newParser(rhs)
	p.L.Next()

	v, err := p.parseSelect()
	if err != nil {
		print(fmt.Sprintf("%s\n", err))
	}
	assert.True(err == nil)
	assert.Equal(lhs, PrintSelect(v))
}

func TestSelect1(t *testing.T) {
	assert := assert.New(t)

	doTestSelect(
		`select
a
from xx()`, "select a from xx()", assert)

	doTestSelect(
		`select
a
from xx(1)`, "select a from xx(1)", assert)

	doTestSelect(
		`select
a
from xx(true, false, 1, 2.000000, "")`, "select a from xx(true, false, 1, 2.0, '')", assert)

	doTestSelect(
		`select
a as t
from xx(true, false, 1, 2.000000, "")`, "select a as t from xx(true, false, 1, 2.0, '')", assert)

	doTestSelect(
		`select
a as t, b as t2
from xx(true, false, 1, 2.000000, "")`, "select a as t, b as t2 from xx(true, false, 1, 2.0, '')", assert)

	doTestSelect(
		`select
($1+10) as t, b as t2
from xx(true, false, 1, 2.000000, "")`, "select $1+10 as t, b as t2 from xx(true, false, 1, 2.0, '')", assert)

	doTestSelect(
		`select distinct
a
from xx(true, false, 1, 2.000000, "")`, "select distinct a from xx(true, false, 1, 2.0, '')", assert)

	doTestSelect(
		`select
max(a)
from xx(true, false, 1, 2.000000, "")`, "select max(a) from xx(true, false, 1, 2.0, '')", assert)

	doTestSelect(
		`select
min(a)
from xx(true, false, 1, 2.000000, "")`, "select min(a) from xx(true, false, 1, 2.0, '')", assert)

	doTestSelect(
		`select distinct
avg(a)
from xx(true, false, 1, 2.000000, "")`, "select distinct avg(a) from xx(true, false, 1, 2.0, '')", assert)

	doTestSelect(
		`select distinct
avg(a)
from xx() as tb1`, "select distinct avg(a) from xx() as tb1", assert)

	doTestSelect(
		`select distinct
avg(a)
from xx() as tb1, yy() as tb2`, "select distinct avg(a) from xx() as tb1, yy() as tb2", assert)

	doTestSelect(
		`select distinct
avg(a) as ttt
from xx() as tb1, yy() as tb2`, "select distinct avg(a) as ttt from xx() as tb1, yy() as tb2", assert)

	doTestSelect(
		`select
a
from yy()
where (a==100)`, "select a from yy() where a == 100", assert)

	doTestSelect(
		`select
a
from yy()
where ((a==100)&&(a!=300))`, "select a from yy() where a == 100 and a != 300", assert)

	doTestSelect(
		`select
a, b as ttt
from yy()
where (((a==100)&&(a!=300))||(ttt>a))`, "select a, b as ttt from yy() where (a == 100 and a != 300) or ttt > a", assert)

	doTestSelect(
		`select
max(t1."a") as f1, avg(t2."b") as f2
from xx() as t1, yy() as t2
where ((f1==f2)&&((f1>20)&&(f1<100)))
group by t1."a", t2."b"
order by t1."c" asc`,
		`
select
max(t1.a) as f1,
avg(t2.b) as f2
from xx() as t1, yy() as t2
where f1 == f2 and (f1 > 20 and f1 < 100)
group by t1.a, t2.b
order by t1.c asc
`,
		assert)

}

func TestExprTernary(t *testing.T) {
	assert := assert.New(t)
	{
		p := newParser("a?b:c")
		p.L.Next()
		v, err := p.parseExpr()
		assert.True(err == nil)
		assert.True(v.Type() == ExprTernary)
		ter := v.(*Ternary)

		assert.True(ter.Cond.Type() == ExprRef)
		assert.True(ter.B0.Type() == ExprRef)
		assert.True(ter.B1.Type() == ExprRef)

		a0 := ter.Cond.(*Ref)
		a1 := ter.B0.(*Ref)
		a2 := ter.B1.(*Ref)

		assert.True(a0.Id == "a")
		assert.True(a1.Id == "b")
		assert.True(a2.Id == "c")
	}

	{
		p := newParser("a?b+e:c")
		p.L.Next()
		v, err := p.parseExpr()
		assert.True(err == nil)
		assert.True(v.Type() == ExprTernary)
		ter := v.(*Ternary)

		assert.True(ter.Cond.Type() == ExprRef)
		assert.True(ter.B0.Type() == ExprBinary)
		assert.True(ter.B1.Type() == ExprRef)

		a0 := ter.Cond.(*Ref)
		a1 := ter.B0.(*Binary)
		a2 := ter.B1.(*Ref)

		assert.True(a0.Id == "a")
		assert.True(a2.Id == "c")
		{
			bin := a1
			assert.True(bin.Op == TkAdd)
			binL := bin.L.(*Ref)
			binR := bin.R.(*Ref)
			assert.True(binL.Id == "b")
			assert.True(binR.Id == "e")
		}
	}
}

func TestExprBinary(t *testing.T) {
	assert := assert.New(t)
	{
		p := newParser("a+b")
		p.L.Next()
		v, err := p.parseExpr()
		assert.True(err == nil)
		assert.True(v.Type() == ExprBinary)
		bin := v.(*Binary)

		assert.True(bin.Op == TkAdd)
		assert.True(bin.L.Type() == ExprRef)
		assert.True(bin.R.Type() == ExprRef)

		lhs := bin.L.(*Ref)
		rhs := bin.R.(*Ref)

		assert.True(lhs.Id == "a")
		assert.True(rhs.Id == "b")
	}

	{
		p := newParser("a-b")
		p.L.Next()
		v, err := p.parseExpr()
		assert.True(err == nil)
		assert.True(v.Type() == ExprBinary)
		bin := v.(*Binary)

		assert.True(bin.Op == TkSub)
		assert.True(bin.L.Type() == ExprRef)
		assert.True(bin.R.Type() == ExprRef)

		lhs := bin.L.(*Ref)
		rhs := bin.R.(*Ref)

		assert.True(lhs.Id == "a")
		assert.True(rhs.Id == "b")
	}

	{
		p := newParser("a-b*c")
		p.L.Next()
		v, err := p.parseExpr()
		assert.True(err == nil)
		assert.True(v.Type() == ExprBinary)
		bin := v.(*Binary)

		assert.True(bin.Op == TkSub)
		assert.True(bin.L.Type() == ExprRef)
		assert.True(bin.R.Type() == ExprBinary)

		lhs := bin.L.(*Ref)
		rhs := bin.R.(*Binary)

		assert.True(lhs.Id == "a")

		assert.True(rhs.L.Type() == ExprRef)
		assert.True(rhs.R.Type() == ExprRef)
		assert.True(rhs.Op == TkMul)

		rhsL := rhs.L.(*Ref)
		rhsR := rhs.R.(*Ref)
		assert.True(rhsL.Id == "b")
		assert.True(rhsR.Id == "c")
	}

	{
		p := newParser("a-b*c+d")
		p.L.Next()
		v, err := p.parseExpr()
		assert.True(err == nil)
		assert.True(v.Type() == ExprBinary)
		assert.Equal(PrintExpr(v), "((a-(b*c))+d)")
	}

	{
		p := newParser("a-b*c*d-e")
		p.L.Next()
		v, err := p.parseExpr()
		assert.True(err == nil)
		assert.True(v.Type() == ExprBinary)
		assert.Equal(PrintExpr(v), "((a-((b*c)*d))-e)")
	}

	{
		p := newParser("a-b*c || d-e && c")
		p.L.Next()
		v, err := p.parseExpr()
		assert.True(err == nil)
		assert.True(v.Type() == ExprBinary)
		assert.Equal(PrintExpr(v), "((a-(b*c))||((d-e)&&c))")
	}

	{
		p := newParser("a == 10 and a == 200 or b == 30")
		p.L.Next()
		print("--------------------------\n")
		v, err := p.parseExpr()
		assert.True(err == nil)
		assert.True(v.Type() == ExprBinary)
		assert.Equal(PrintExpr(v), "(((a==10)&&(a==200))||(b==30))")
	}
}

func TestExprUnary(t *testing.T) {
	assert := assert.New(t)
	{
		p := newParser("-a")
		p.L.Next()
		v, err := p.parseExpr()
		assert.True(err == nil)
		assert.True(v.Type() == ExprUnary)
		u := v.(*Unary)
		assert.True(len(u.Op) == 1)
		assert.True(u.Op[0] == TkSub)
		assert.True(u.Operand.Type() == ExprRef)
		ref := u.Operand.(*Ref)
		assert.True(ref.Id == "a")
	}
	{
		p := newParser("--a")
		p.L.Next()
		v, err := p.parseExpr()
		assert.True(err == nil)
		assert.True(v.Type() == ExprUnary)
		u := v.(*Unary)
		assert.True(len(u.Op) == 2)
		assert.True(u.Op[0] == TkSub)
		assert.True(u.Op[1] == TkSub)

		assert.True(u.Operand.Type() == ExprRef)
		ref := u.Operand.(*Ref)
		assert.True(ref.Id == "a")
	}
	{
		p := newParser("!!a")
		p.L.Next()
		v, err := p.parseExpr()
		assert.True(err == nil)
		assert.True(v.Type() == ExprUnary)
		u := v.(*Unary)
		assert.True(len(u.Op) == 2)
		assert.True(u.Op[0] == TkNot)
		assert.True(u.Op[1] == TkNot)

		assert.True(u.Operand.Type() == ExprRef)
		ref := u.Operand.(*Ref)
		assert.True(ref.Id == "a")
	}
	{
		p := newParser("++a")
		p.L.Next()
		v, err := p.parseExpr()
		assert.True(err == nil)
		assert.True(v.Type() == ExprUnary)
		u := v.(*Unary)
		assert.True(len(u.Op) == 2)
		assert.True(u.Op[0] == TkAdd)
		assert.True(u.Op[1] == TkAdd)

		assert.True(u.Operand.Type() == ExprRef)
		ref := u.Operand.(*Ref)
		assert.True(ref.Id == "a")
	}
}

func TestExprSub(t *testing.T) {
	assert := assert.New(t)
	{
		p := newParser("(a+b)")
		p.L.Next()
		v, err := p.parseExpr()
		assert.True(err == nil)
		assert.True(PrintExpr(v) == "(a+b)")
	}
	{
		p := newParser("a*(a+b)+c")
		p.L.Next()
		v, err := p.parseExpr()
		assert.True(err == nil)
		assert.True(PrintExpr(v) == "((a*(a+b))+c)")
	}
}

func TestExprPrimary(t *testing.T) {
	assert := assert.New(t)
	{
		p := newParser("a.b")
		p.L.Next()
		v, err := p.parseExpr()
		assert.True(err == nil)
		assert.True(v.Type() == ExprPrimary)
		primary := v.(*Primary)
		assert.True(primary.Leading.Type() == ExprRef)
		ref := primary.Leading.(*Ref)
		assert.True(ref.Id == "a")

		assert.True(len(primary.Suffix) == 1)
		assert.True(primary.Suffix[0].Ty == SuffixDot)
		assert.True(primary.Suffix[0].Component == "b")
	}
	{
		p := newParser("a.'b'")
		p.L.Next()
		v, err := p.parseExpr()
		assert.True(err == nil)
		assert.True(v.Type() == ExprPrimary)
		primary := v.(*Primary)
		assert.True(primary.Leading.Type() == ExprRef)
		ref := primary.Leading.(*Ref)
		assert.True(ref.Id == "a")

		assert.True(len(primary.Suffix) == 1)
		assert.True(primary.Suffix[0].Ty == SuffixDot)
		assert.True(primary.Suffix[0].Component == "b")
	}
	{
		p := newParser("a[1]")
		p.L.Next()
		v, err := p.parseExpr()
		assert.True(err == nil)
		assert.True(v.Type() == ExprPrimary)
		primary := v.(*Primary)
		assert.True(primary.Leading.Type() == ExprRef)
		ref := primary.Leading.(*Ref)
		assert.True(ref.Id == "a")

		assert.True(len(primary.Suffix) == 1)
		assert.True(primary.Suffix[0].Ty == SuffixIndex)
		idx := primary.Suffix[0].Index
		assert.True(idx.Type() == ExprConst)
		cidx := idx.(*Const)
		assert.True(cidx.Ty == ConstInt)
		assert.True(cidx.Int == int64(1))
	}
	{
		p := newParser("a['']")
		p.L.Next()
		v, err := p.parseExpr()
		assert.True(err == nil)
		assert.True(v.Type() == ExprPrimary)
		primary := v.(*Primary)
		assert.True(primary.Leading.Type() == ExprRef)
		ref := primary.Leading.(*Ref)
		assert.True(ref.Id == "a")

		assert.True(len(primary.Suffix) == 1)
		assert.True(primary.Suffix[0].Ty == SuffixIndex)
		idx := primary.Suffix[0].Index
		assert.True(idx.Type() == ExprConst)
		cidx := idx.(*Const)
		assert.True(cidx.Ty == ConstStr)
		assert.True(cidx.String == "")
	}
	{
		p := newParser("a()")
		p.L.Next()
		v, err := p.parseExpr()
		assert.True(err == nil)
		assert.True(v.Type() == ExprPrimary)
		primary := v.(*Primary)
		assert.True(primary.Leading.Type() == ExprRef)
		ref := primary.Leading.(*Ref)
		assert.True(ref.Id == "a")

		assert.True(len(primary.Suffix) == 1)

		assert.True(primary.Suffix[0].Ty == SuffixCall)
		call := primary.Suffix[0].Call
		assert.True(len(call.Parameters) == 0)
	}
	{
		p := newParser("a(1)")
		p.L.Next()
		v, err := p.parseExpr()
		assert.True(err == nil)
		assert.True(v.Type() == ExprPrimary)
		primary := v.(*Primary)
		assert.True(primary.Leading.Type() == ExprRef)
		ref := primary.Leading.(*Ref)
		assert.True(ref.Id == "a")

		assert.True(len(primary.Suffix) == 1)

		assert.True(primary.Suffix[0].Ty == SuffixCall)
		call := primary.Suffix[0].Call
		assert.True(len(call.Parameters) == 1)
		assert.True(call.Parameters[0].Type() == ExprConst)
		c0 := call.Parameters[0].(*Const)
		assert.True(c0.Ty == ConstInt)
		assert.True(c0.Int == int64(1))
	}
	{
		p := newParser("a(1,)")
		p.L.Next()
		v, err := p.parseExpr()
		assert.True(err == nil)
		assert.True(v.Type() == ExprPrimary)
		primary := v.(*Primary)
		assert.True(primary.Leading.Type() == ExprRef)
		ref := primary.Leading.(*Ref)
		assert.True(ref.Id == "a")

		assert.True(len(primary.Suffix) == 1)

		assert.True(primary.Suffix[0].Ty == SuffixCall)
		call := primary.Suffix[0].Call
		assert.True(len(call.Parameters) == 1)
		assert.True(call.Parameters[0].Type() == ExprConst)
		c0 := call.Parameters[0].(*Const)
		assert.True(c0.Ty == ConstInt)
		assert.True(c0.Int == int64(1))
	}
	{
		p := newParser("a(1,2)")
		p.L.Next()
		v, err := p.parseExpr()
		assert.True(err == nil)
		assert.True(v.Type() == ExprPrimary)
		primary := v.(*Primary)
		assert.True(primary.Leading.Type() == ExprRef)
		ref := primary.Leading.(*Ref)
		assert.True(ref.Id == "a")

		assert.True(len(primary.Suffix) == 1)

		assert.True(primary.Suffix[0].Ty == SuffixCall)
		call := primary.Suffix[0].Call
		assert.True(len(call.Parameters) == 2)

		assert.True(call.Parameters[0].Type() == ExprConst)
		c0 := call.Parameters[0].(*Const)
		assert.True(c0.Ty == ConstInt)
		assert.True(c0.Int == int64(1))

		assert.True(call.Parameters[1].Type() == ExprConst)
		c1 := call.Parameters[1].(*Const)
		assert.True(c1.Ty == ConstInt)
		assert.True(c1.Int == int64(2))
	}
}

func TestExprConst(t *testing.T) {
	assert := assert.New(t)
	{
		p := newParser("1")
		p.L.Next()
		v, err := p.parseExpr()
		assert.True(err == nil)
		assert.True(v.Type() == ExprConst)
		c := v.(*Const)
		assert.True(c.Ty == ConstInt)
		assert.Equal(c.Int, int64(1), "int == 1")
	}
	{
		p := newParser("1.1")
		p.L.Next()
		v, err := p.parseExpr()
		assert.True(err == nil)
		assert.True(v.Type() == ExprConst)
		c := v.(*Const)
		assert.True(c.Ty == ConstReal)
		assert.Equal(c.Real, float64(1.1), "real == 1.1")
	}
	{
		p := newParser("'str'")
		p.L.Next()
		v, err := p.parseExpr()
		assert.True(err == nil)
		assert.True(v.Type() == ExprConst)
		c := v.(*Const)
		assert.True(c.Ty == ConstStr)
		assert.Equal(c.String, "str", "str == 'str'")
	}
	{
		p := newParser("null")
		p.L.Next()
		v, err := p.parseExpr()
		assert.True(err == nil)
		assert.True(v.Type() == ExprConst)
		c := v.(*Const)
		assert.True(c.Ty == ConstNull)
	}
	{
		p := newParser("true")
		p.L.Next()
		v, err := p.parseExpr()
		assert.True(err == nil)
		assert.True(v.Type() == ExprConst)
		c := v.(*Const)
		assert.True(c.Ty == ConstBool)
		assert.True(c.Bool)
	}
	{
		p := newParser("false")
		p.L.Next()
		v, err := p.parseExpr()
		assert.True(err == nil)
		assert.True(v.Type() == ExprConst)
		c := v.(*Const)
		assert.True(c.Ty == ConstBool)
		assert.False(c.Bool)
	}
}

func TestExprAtomic(t *testing.T) {
	assert := assert.New(t)
	{
		p := newParser("a")
		p.L.Next()
		v, err := p.parseExpr()
		assert.True(err == nil)
		assert.True(v.Type() == ExprRef)
		ref := v.(*Ref)
		assert.Equal(ref.Id, "a")
	}
}
