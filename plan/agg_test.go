package plan

import (
	"github.com/dianpeng/sql2awk/sql"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAggProjection(t *testing.T) {
	assert := assert.New(t)
	// index
	{
		s := compAST(
			`
select min(t1.$1) as f1,
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
		assert.True(p.anaAgg(s) == nil)
		assert.True(p.HasAgg())

		assert.Equal(len(s.Projection.ValueList), 2)

		v0 := s.Projection.ValueList[0]
		assert.Equal(v0.Type(), sql.SelectVarCol)
		col := v0.(*sql.Col)
		agg := col.Value

		// resolved to aggregation reference, instead of the free one
		{
			assert.Equal(agg.Type(), sql.ExprPrimary)
			primary := agg.(*sql.Primary)
			assert.True(primary.CanName.IsTableColumn())
			assert.Equal(primary.CanName.TableIndex, aggTableIndex)
			assert.Equal(primary.CanName.ColumnIndex, 0)
		}

		{
			agg := p.aggExpr[0]
			assert.Equal(agg.AggType, AggMin)
			assert.Equal(sql.PrintExpr(agg.Value),
				`(t1."$1")`,
			)
		}
	}

	{
		s := compAST(
			`
select min(t1.$1 + 100) as f1,
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
		assert.True(p.anaAgg(s) == nil)
		assert.True(p.HasAgg())

		assert.Equal(len(s.Projection.ValueList), 2)

		v0 := s.Projection.ValueList[0]
		assert.Equal(v0.Type(), sql.SelectVarCol)
		col := v0.(*sql.Col)
		agg := col.Value

		// resolved to aggregation reference, instead of the free one
		{
			assert.Equal(agg.Type(), sql.ExprPrimary)
			primary := agg.(*sql.Primary)
			assert.True(primary.CanName.IsTableColumn())
			assert.Equal(primary.CanName.TableIndex, aggTableIndex)
			assert.Equal(primary.CanName.ColumnIndex, 0)
		}

		{
			agg := p.aggExpr[0]
			assert.Equal(agg.AggType, AggMin)
			assert.Equal(sql.PrintExpr(agg.Value),
				`((t1."$1"+100))`,
			)
		}
	}

	{
		s := compAST(
			`
select min(t1.$1) + 100 as f1,
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
		assert.True(p.anaAgg(s) == nil)
		assert.True(p.HasAgg())

		assert.Equal(len(s.Projection.ValueList), 2)

		v0 := s.Projection.ValueList[0]
		assert.Equal(v0.Type(), sql.SelectVarCol)
		col := v0.(*sql.Col)
		agg := col.Value

		// resolved to aggregation reference, instead of the free one
		{
			assert.Equal(agg.Type(), sql.ExprBinary)
			binary := agg.(*sql.Binary)
			assert.Equal(binary.L.Type(), sql.ExprPrimary)
			primary := binary.L.(*sql.Primary)
			assert.True(primary.CanName.IsTableColumn())
			assert.Equal(primary.CanName.TableIndex, aggTableIndex)
			assert.Equal(primary.CanName.ColumnIndex, 0)
		}

		{
			agg := p.aggExpr[0]
			assert.Equal(agg.AggType, AggMin)
			assert.Equal(sql.PrintExpr(agg.Value),
				`(t1."$1")`,
			)
		}
	}
}

func TestAggHaving(t *testing.T) {
	assert := assert.New(t)
	// index
	{
		s := compAST(
			`
select min(t1.$1) as f1,
       t2.$2 as f2

from tab("/a/b/1") as t1,
     tab("/a/b/2") as t2

having max(t2.$10) > 10
`,
		)
		assert.True(s != nil)
		p := newPlan()
		err := p.scanTableAndResolveSymbol(s) // this is required
		assert.True(err == nil)
		assert.True(p.anaAgg(s) == nil)
		assert.True(p.HasAgg())

		{
			having := s.Having.Condition
			assert.Equal(having.Type(), sql.ExprBinary)
			binary := having.(*sql.Binary)
			lhs := binary.L
			assert.Equal(lhs.Type(), sql.ExprPrimary)
			primary := lhs.(*sql.Primary)
			assert.True(primary.CanName.IsTableColumn())
			assert.Equal(primary.CanName.TableIndex, aggTableIndex)
			assert.Equal(primary.CanName.ColumnIndex, 1)
		}
	}

	{
		s := compAST(
			`
select min(t1.$1) as f1,
       t2.$2 as f2

from tab("/a/b/1") as t1,
     tab("/a/b/2") as t2

having max(t2.$10 + 1) > 10
`,
		)
		assert.True(s != nil)
		p := newPlan()
		err := p.scanTableAndResolveSymbol(s) // this is required
		assert.True(err == nil)
		assert.True(p.anaAgg(s) == nil)
		assert.True(p.HasAgg())
		assert.Equal(len(p.aggExpr), 2)

		{
			having := s.Having.Condition
			assert.Equal(having.Type(), sql.ExprBinary)
			binary := having.(*sql.Binary)
			lhs := binary.L
			assert.Equal(lhs.Type(), sql.ExprPrimary)
			primary := lhs.(*sql.Primary)
			assert.True(primary.CanName.IsTableColumn())
			assert.Equal(primary.CanName.TableIndex, aggTableIndex)
			assert.Equal(primary.CanName.ColumnIndex, 1)
		}
	}

	{
		s := compAST(
			`
select min(t1.$1) as f1,
       t2.$2 as f2

from tab("/a/b/1") as t1,
     tab("/a/b/2") as t2

having (max(t2.$10) + 1) > 10
`,
		)
		assert.True(s != nil)
		p := newPlan()
		err := p.scanTableAndResolveSymbol(s) // this is required
		assert.True(err == nil)
		assert.True(p.anaAgg(s) == nil)
		assert.True(p.HasAgg())
		assert.Equal(len(p.aggExpr), 2)

		{
			having := s.Having.Condition
			assert.Equal(having.Type(), sql.ExprBinary)
			binary := having.(*sql.Binary)
			lhs := binary.L
			assert.Equal(lhs.Type(), sql.ExprBinary)
			lhs = lhs.(*sql.Binary).L
			assert.Equal(lhs.Type(), sql.ExprPrimary)
			primary := lhs.(*sql.Primary)
			assert.True(primary.CanName.IsTableColumn())
			assert.Equal(primary.CanName.TableIndex, aggTableIndex)
			assert.Equal(primary.CanName.ColumnIndex, 1)
		}
	}
}

func TestAggOrderBy(t *testing.T) {
	assert := assert.New(t)
	// index
	{
		s := compAST(
			`
select min(t1.$1) as f1,
       t2.$2 as f2

from tab("/a/b/1") as t1,
     tab("/a/b/2") as t2

order by max(t2.$10), avg(t1.$10) asc
`,
		)
		assert.True(s != nil)
		p := newPlan()
		err := p.scanTableAndResolveSymbol(s) // this is required
		assert.True(err == nil)
		assert.True(p.anaAgg(s) == nil)
		assert.True(p.HasAgg())
		assert.Equal(len(p.aggExpr), 3)
	}
}
