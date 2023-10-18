package plan

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

// testing the semantic issue of the SQL

func TestSemaGroupBy(t *testing.T) {
	assert := assert.New(t)
	{
		// 1) group by with expression covers the one that is not in the
		//    agg
		s := compAST(
			`
select $1, avg($2)
from tab("sample")
group by $1
`)
		p := newPlan()
		err := p.planPrepare(s)
		assert.True(err == nil)
	}

	{
		// 2) group by with expression covers the one that is not in the
		//    agg. The group by term is used as expression
		s := compAST(
			`
select $1, $3, avg($2)
from tab("sample")
group by $1, ($3 + 100) + $1
`)
		p := newPlan()
		err := p.planPrepare(s)
		assert.True(err == nil)
	}

	{
		// 3) group by with expression has more than it is supposed to cover, this
		//    is allowed either
		s := compAST(
			`
select $1, $3, avg($2)
from tab("sample")
group by $1, ($3 + 100) + $4
`)
		p := newPlan()
		err := p.planPrepare(s)
		assert.True(err == nil)
	}

	{
		// 4) failed with semantic checking, the group by does not cover enough
		s := compAST(
			`
select $1, $3, avg($2)
from tab("sample")
group by $1
`)
		p := newPlan()
		err := p.planPrepare(s)
		assert.True(err != nil)
	}

	{
		// 5) no group by
		s := compAST(
			`
select $1, $3, avg($2)
from tab("sample")
`)
		p := newPlan()
		err := p.planPrepare(s)
		assert.True(err != nil)
	}
}

func TestSemaWildcard(t *testing.T) {
	assert := assert.New(t)
	{
		// 1) normal wildcard search
		s := compAST(
			`
select *
from tab("sample")
`)
		p := newPlan()
		err := p.planPrepare(s)
		assert.True(err == nil)
	}

	{
		// 2) wildcard search with group by
		s := compAST(
			`
select *
from tab("sample")
group by $2
`)
		p := newPlan()
		err := p.planPrepare(s)
		assert.True(err == nil)
	}

	{
		// 3) invalid having
		s := compAST(
			`
select *
from tab("sample")
group by $2
having min($3)
`)
		p := newPlan()
		err := p.planPrepare(s)
		assert.True(err != nil)
	}
}
