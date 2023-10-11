package plan

import (
	"github.com/dianpeng/sql2awk/sql"
	"github.com/stretchr/testify/assert"
	"testing"
)

/*

func TestPlan1(t *testing.T) {
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
    p.plan(s)
    print(p.Print())
    assert.True(false)
	}

	{
		s := compAST(
			`
select t1.$1 as f1,
       t2.$2 as f2

from tab("/a/b/1") as t1,
     tab("/a/b/2") as t2

where (f1 == 100) and (f2 == 200) and (f1 == f2)
`,
		)
		assert.True(s != nil)
		p := newPlan()
    p.plan(s)
    print(p.Print())
    assert.True(false)
	}
}

*/

// test the specific join filter, which is a tree split algorithm to pick up
// what we left during table scane's early filter.
func TestJoinFilter(t *testing.T) {
	assert := assert.New(t)
	one := func(code string, expr string) {
		s := compAST(code)
		assert.True(s != nil)
		p := newPlan()
		p.plan(s)
		assert.True(p.Join != nil)
		filter := p.Join.JoinFilter()
		assert.Equal(expr, sql.PrintExpr(filter))
	}

	one(
		`
select t1.$1 as f1,
       t2.$2 as f2

from tab("/a/b/1") as t1,
     tab("/a/b/2") as t2

where (100 != 200)
`,
		"",
	)

	one(
		`
select t1.$1 as f1,
       t2.$2 as f2

from tab("/a/b/1") as t1,
     tab("/a/b/2") as t2

where f1 == f2 && (f1 == 100 and f2 == 200)
`,
		"(f1==f2)",
	)
	one(
		`
select t1.$1 as f1,
       t2.$2 as f2

from tab("/a/b/1") as t1,
     tab("/a/b/2") as t2

where f1 == f2 && f2 == 100
`,
		"(f1==f2)",
	)
}
