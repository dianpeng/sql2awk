package plan

// expression analysis testing, basically just testing out the freaking filter

import (
	"github.com/dianpeng/sql2awk/sql"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestExprSuffix(t *testing.T) {
	assert := assert.New(t)
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
		err := p.resolveSymbol(s) // this is required
		assert.True(err == nil)

		cond := s.Where.Condition
		info := newExprTableAccessInfo(cond)
		assert.Equal(cond.Type(), sql.ExprPrimary)

		{
			x := info.info[cond]
			assert.Equal(len(x), 2)
			assert.True(x[0])
			assert.True(x[1])
		}
	}
}

func TestExprBinary(t *testing.T) {
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
		err := p.resolveSymbol(s) // this is required
		assert.True(err == nil)

		cond := s.Where.Condition
		info := newExprTableAccessInfo(cond)

		{
			x := info.info[cond]
			assert.Equal(len(x), 2)
			assert.True(x[0])
			assert.True(x[1])
		}

		bin := cond.(*sql.Binary)
		{
			l := bin.L
			x := info.info[l]
			assert.True(x.Has(0))
			assert.Equal(len(x), 1)
			assert.True(x[0])
		}

		{
			r := bin.R
			x := info.info[r]
			assert.True(x.Has(1))
			assert.Equal(len(x), 1)
			assert.True(x[1])
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
		err := p.resolveSymbol(s) // this is required
		assert.True(err == nil)

		cond := s.Where.Condition
		info := newExprTableAccessInfo(cond)

		{
			x := info.info[cond]
			assert.Equal(len(x), 2)
			assert.True(x[0])
			assert.True(x[1])
		}

		bin := cond.(*sql.Binary)
		{
			l := bin.L
			x := info.info[l]
			assert.True(x.Has(0))
			assert.Equal(len(x), 1)
			assert.True(x[0])
		}

		{
			r := bin.R
			x := info.info[r]
			assert.True(x.Has(1))
			assert.Equal(len(x), 1)
			assert.True(x[1])
		}
	}

	{
		s := compAST(
			`
select t1.$1 as f1,
       t2.$2 as f2,
       t3.$2 as f3,
       t4.$2 as f4,
       t5.$2 as f5

from tab("/a/b/1") as t1,
     tab("/a/b/2") as t2,
     tab("/a/b/3") as t3,
     tab("/a/b/4") as t4,
     tab("/a/b/5") as t5

where (f1 == f2 && f2 == (f3 + f4)) || f5
`,
		)
		assert.True(s != nil)
		p := newPlan()
		err := p.resolveSymbol(s) // this is required
		assert.True(err == nil)

		cond := s.Where.Condition
		info := newExprTableAccessInfo(cond)

		{
			x := info.info[cond]
			assert.Equal(len(x), 5)
			assert.True(x.Has(0))
			assert.True(x.Has(1))
			assert.True(x.Has(2))
			assert.True(x.Has(3))
			assert.True(x.Has(4))
		}
	}
}
