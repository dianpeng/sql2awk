package plan

import (
	"github.com/dianpeng/sql2awk/sql"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEarlyFilterRest(t *testing.T) {
	assert := assert.New(t)
	// index
	{
		s := compAST(
			`
select t1.$1 as f1,
       t2.$2 as f2

from tab("/a/b/1") as t1,
     tab("/a/b/2") as t2

where foo(f1, f2)
`,
		)
		assert.True(s != nil)
		p := newPlan()
		err := p.scanTableAndResolveSymbol(s) // this is required
		assert.True(err == nil)

		cond := s.Where.Condition
		info := newExprTableAccessInfo(cond)
		{
			ef := p.anaEarlyFilter(
				0,
				info,
				cond,
			)
			assert.True(ef == nil)
		}

		{
			ef := p.anaEarlyFilter(
				1,
				info,
				cond,
			)
			assert.True(ef == nil)
		}
	}
}

func TestEarlyFilterStatic(t *testing.T) {
	assert := assert.New(t)
	{
		s := compAST(
			`
select t1.$1 as f1,
       t2.$2 as f2

from tab("/a/b/1") as t1,
     tab("/a/b/2") as t2

where 100 != 200
`,
		)
		assert.True(s != nil)
		p := newPlan()
		err := p.scanTableAndResolveSymbol(s) // this is required
		assert.True(err == nil)

		cond := s.Where.Condition
		info := newExprTableAccessInfo(cond)
		{
			ef := p.anaEarlyFilter(
				0,
				info,
				cond,
			)
			assert.True(ef != nil)
			assert.Equal(sql.PrintExpr(ef), "(100!=200)")
		}

		{
			ef := p.anaEarlyFilter(
				1,
				info,
				cond,
			)
			assert.True(ef != nil)
			assert.Equal(sql.PrintExpr(ef), "(100!=200)")
		}
	}
}

func TestEarlyFilterLogic(t *testing.T) {
	assert := assert.New(t)
	{
		s := compAST(
			`
select t1.$1 as f1,
       t2.$2 as f2

from tab("/a/b/1") as t1,
     tab("/a/b/2") as t2

where (f1 == 100 && f2 == 200)
`,
		)
		assert.True(s != nil)
		p := newPlan()
		err := p.scanTableAndResolveSymbol(s) // this is required
		assert.True(err == nil)

		cond := s.Where.Condition
		info := newExprTableAccessInfo(cond)
		{
			ef := p.anaEarlyFilter(
				0,
				info,
				cond,
			)
			assert.True(ef != nil)
			assert.Equal(sql.PrintExpr(ef), "(f1==100)")
		}

		{
			ef := p.anaEarlyFilter(
				1,
				info,
				cond,
			)
			assert.True(ef != nil)
			assert.Equal(sql.PrintExpr(ef), "(f2==200)")
		}
	}

	{
		s := compAST(
			`
select t1.$1 as f1,
       t2.$2 as f2

from tab("/a/b/1") as t1,
     tab("/a/b/2") as t2

where (f1 == f2)
`,
		)
		assert.True(s != nil)
		p := newPlan()
		err := p.scanTableAndResolveSymbol(s) // this is required
		assert.True(err == nil)

		cond := s.Where.Condition
		info := newExprTableAccessInfo(cond)
		{
			ef := p.anaEarlyFilter(
				0,
				info,
				cond,
			)
			assert.True(ef == nil)
		}

		{
			ef := p.anaEarlyFilter(
				1,
				info,
				cond,
			)
			assert.True(ef == nil)
		}
	}

	{
		s := compAST(
			`
select t1.$1 as f1,
       t2.$2 as f2

from tab("/a/b/1") as t1,
     tab("/a/b/2") as t2

where (t1.$1 == t2.$2)
`,
		)
		assert.True(s != nil)
		p := newPlan()
		err := p.scanTableAndResolveSymbol(s) // this is required
		assert.True(err == nil)

		cond := s.Where.Condition
		info := newExprTableAccessInfo(cond)
		{
			ef := p.anaEarlyFilter(
				0,
				info,
				cond,
			)
			assert.True(ef == nil)
		}

		{
			ef := p.anaEarlyFilter(
				1,
				info,
				cond,
			)
			assert.True(ef == nil)
		}
	}

	{
		s := compAST(
			`
select t1.$1 as f1,
       t2.$2 as f2

from tab("/a/b/1") as t1,
     tab("/a/b/2") as t2

where (f1 == 100 && f2 == f1)
`,
		)
		assert.True(s != nil)
		p := newPlan()
		err := p.scanTableAndResolveSymbol(s) // this is required
		assert.True(err == nil)

		cond := s.Where.Condition
		info := newExprTableAccessInfo(cond)

		// (f1 == 100) is a conservative approximation of (f1 == 100 && f2 == f1)
		{
			ef := p.anaEarlyFilter(
				0,
				info,
				cond,
			)
			assert.True(ef != nil)
			assert.Equal(sql.PrintExpr(ef), "(f1==100)")
		}

		{
			ef := p.anaEarlyFilter(
				1,
				info,
				cond,
			)
			assert.True(ef == nil)
		}
	}

	{
		s := compAST(
			`
select t1.$1 as f1,
       t2.$2 as f2

from tab("/a/b/1") as t1,
     tab("/a/b/2") as t2

where (f1 == 100 or f2 == f1)
`,
		)
		assert.True(s != nil)
		p := newPlan()
		err := p.scanTableAndResolveSymbol(s) // this is required
		assert.True(err == nil)

		cond := s.Where.Condition
		info := newExprTableAccessInfo(cond)

		// (f1 == 100) is a conservative approximation of (f1 == 100 && f2 == f1)
		{
			ef := p.anaEarlyFilter(
				0,
				info,
				cond,
			)
			assert.True(ef == nil)
		}

		{
			ef := p.anaEarlyFilter(
				1,
				info,
				cond,
			)
			assert.True(ef == nil)
		}
	}

	{
		s := compAST(
			`
select t1.$1 as f1,
       t2.$2 as f2

from tab("/a/b/1") as t1,
     tab("/a/b/2") as t2

where (f1 == 100 and (f1 == 300 or f1 == 100)) and (f2 == f1 or f2 == 800)
`,
		)
		assert.True(s != nil)
		p := newPlan()
		err := p.scanTableAndResolveSymbol(s) // this is required
		assert.True(err == nil)

		cond := s.Where.Condition
		info := newExprTableAccessInfo(cond)

		// (f1 == 100) is a conservative approximation of (f1 == 100 && f2 == f1)
		{
			ef := p.anaEarlyFilter(
				0,
				info,
				cond,
			)
			assert.True(ef != nil)
			assert.Equal(
				sql.PrintExpr(ef),
				"((f1==100)&&((f1==300)||(f1==100)))",
			)
		}

		{
			ef := p.anaEarlyFilter(
				1,
				info,
				cond,
			)
			assert.True(ef == nil)
		}
	}

	{
		s := compAST(
			`
select t1.$1 as f1,
       t2.$2 as f2

from tab("/a/b/1") as t1,
     tab("/a/b/2") as t2

where (f1 == 100 or f1 == 200) and f2
`,
		)
		assert.True(s != nil)
		p := newPlan()
		err := p.scanTableAndResolveSymbol(s) // this is required
		assert.True(err == nil)

		cond := s.Where.Condition
		info := newExprTableAccessInfo(cond)

		// (f1 == 100) is a conservative approximation of (f1 == 100 && f2 == f1)
		{
			ef := p.anaEarlyFilter(
				0,
				info,
				cond,
			)
			assert.True(ef != nil)
			assert.Equal(
				sql.PrintExpr(ef),
				"((f1==100)||(f1==200))",
			)
		}

		{
			ef := p.anaEarlyFilter(
				1,
				info,
				cond,
			)
			assert.True(ef != nil)
			assert.Equal(
				sql.PrintExpr(ef),
				"f2",
			)
		}
	}

	{
		s := compAST(
			`
select t1.$1 as f1,
       t2.$2 as f2

from tab("/a/b/1") as t1,
     tab("/a/b/2") as t2

where (f1 == f2 and f1 == 200) and (f1 == 400)
`,
		)
		assert.True(s != nil)
		p := newPlan()
		err := p.scanTableAndResolveSymbol(s) // this is required
		assert.True(err == nil)

		cond := s.Where.Condition
		info := newExprTableAccessInfo(cond)

		// (f1 == 100) is a conservative approximation of (f1 == 100 && f2 == f1)
		{
			ef := p.anaEarlyFilter(
				0,
				info,
				cond,
			)
			assert.True(ef != nil)
			assert.Equal(
				sql.PrintExpr(ef),
				"((f1==200)&&(f1==400))",
			)
		}

		{
			ef := p.anaEarlyFilter(
				1,
				info,
				cond,
			)
			assert.True(ef == nil)
		}
	}

	{
		s := compAST(
			`
select t1.$1 as f1,
       t2.$2 as f2

from tab("/a/b/1") as t1,
     tab("/a/b/2") as t2

where (f1 == f2 and f1 == 200) and (f2 == 400)
`,
		)
		assert.True(s != nil)
		p := newPlan()
		err := p.scanTableAndResolveSymbol(s) // this is required
		assert.True(err == nil)

		cond := s.Where.Condition
		info := newExprTableAccessInfo(cond)

		// (f1 == 100) is a conservative approximation of (f1 == 100 && f2 == f1)
		{
			ef := p.anaEarlyFilter(
				0,
				info,
				cond,
			)
			assert.True(ef != nil)
			assert.Equal(
				sql.PrintExpr(ef),
				"(f1==200)",
			)
		}

		{
			ef := p.anaEarlyFilter(
				1,
				info,
				cond,
			)
			assert.True(ef != nil)
			assert.Equal(
				sql.PrintExpr(ef),
				"(f2==400)",
			)
		}
	}
}
