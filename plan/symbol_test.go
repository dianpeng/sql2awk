package plan

import (
	"fmt"
	"github.com/dianpeng/sql2awk/sql"
	"github.com/stretchr/testify/assert"
	"testing"
)

func compAST(
	code string,
) *sql.Select {
	p := sql.NewParser(code)
	c, err := p.Parse()
	if err != nil {
		print(fmt.Sprintf("Parsing error: %s\n", err))
		return nil
	}
	return c.Select
}

func TestScanTable(t *testing.T) {
	assert := assert.New(t)
	{
		s := compAST(
			`
select t1.$1 as f
from tab("/a/b/1") as t1,
     tab("/a/b/2") as t2,
     tab("/a/b/3") as t3
`,
		)
		assert.True(s != nil)
		p := newPlan()
		err := p.resolveSymbol(s)
		assert.True(err == nil)

		{
			assert.Equal(
				len(p.tableList),
				3,
			)
			{
				t := p.tableList[0]
				assert.Equal(t.Path, "/a/b/1")
				assert.Equal(t.Type, "tab")
				assert.Equal(t.MaxColumn, 1)
				assert.False(t.FullColumn)
				assert.Equal(t.Alias, "t1")
			}

			{
				t := p.tableList[1]
				assert.Equal(t.Path, "/a/b/2")
				assert.Equal(t.Type, "tab")
				assert.Equal(t.MaxColumn, -1)
				assert.False(t.FullColumn)
				assert.Equal(t.Alias, "t2")
			}

			{
				t := p.tableList[2]
				assert.Equal(t.Path, "/a/b/3")
				assert.Equal(t.Type, "tab")
				assert.Equal(t.MaxColumn, -1)
				assert.False(t.FullColumn)
				assert.Equal(t.Alias, "t3")
			}
		}

	}
}

func TestTableDescriptor(t *testing.T) {
	assert := assert.New(t)
	{
		s := compAST(
			`
select t1.$1 as f
from tab("/a/b/c") as t1,
     tab("/a/b/d") as t2,
     tab("/a/b/d") as t3
`,
		)
		assert.True(s != nil)
		p := newPlan()
		err := p.resolveSymbol(s)
		assert.True(err == nil)
		assert.True(
			p.findTableDescriptorByAlias("t1") != nil,
		)
		assert.True(
			p.findTableDescriptorByAlias("t2") != nil,
		)
		assert.True(
			p.findTableDescriptorByAlias("t3") != nil,
		)
		assert.True(
			p.findTableDescriptorByAlias("t4") == nil,
		)
	}
}

// testing symbol resolution
func TestCanNameWhere(t *testing.T) {
	assert := assert.New(t)
	{
		s := compAST(
			`
select t1.$1 as f
from tab("/a/b/c") as t1
where f == 100
`,
		)
		assert.True(s != nil)
		p := newPlan()
		err := p.resolveSymbol(s)
		assert.True(err == nil)

		// the t1 table should be existed
		{
			assert.Equal(
				len(p.tableList),
				1,
			)
			t := p.tableList[0]
			assert.Equal(t.MaxColumn, 1)
			assert.False(t.FullColumn)
			assert.Equal(t.Alias, "t1")
		}

		// the t1.$1 should be resolved
		{
			x := s.Projection.ValueList[0]
			assert.Equal(x.Type(), sql.SelectVarCol)
			col := x.(*sql.Col)
			v := col.Value
			assert.Equal(v.Type(), sql.ExprPrimary)
			primary := v.(*sql.Primary)
			assert.True(primary.CanName.IsSettled())
			assert.True(primary.CanName.IsTableColumn())
			assert.Equal(primary.CanName.TableIndex, 0)
			assert.Equal(primary.CanName.ColumnIndex, 1)
		}

		// the f should be resolved, ie as an alias pointed to the correct one
		{
			f := s.Where.Condition
			assert.Equal(f.Type(), sql.ExprBinary)
			bin := f.(*sql.Binary)
			lhs := bin.L
			assert.Equal(lhs.Type(), sql.ExprRef)
			ref := lhs.(*sql.Ref)
			assert.True(ref.CanName.IsTableColumn())
			assert.Equal(ref.CanName.TableIndex, 0)
			assert.Equal(ref.CanName.ColumnIndex, 1)
		}
	}

	{
		s := compAST(
			`
select (t1.$1) as f1,
       (t1.$2 + 200) as f2
from tab("/a/b/c") as t1
where f2 == 100
`,
		)
		assert.True(s != nil)
		p := newPlan()
		err := p.resolveSymbol(s)
		assert.True(err == nil)

		// the t1 table should be existed
		{
			assert.Equal(
				len(p.tableList),
				1,
			)
			t := p.tableList[0]
			assert.Equal(t.MaxColumn, 2)
			assert.False(t.FullColumn)
			assert.Equal(t.Alias, "t1")
		}

		// the t1.$1 should be resolved
		{
			assert.Equal(len(s.Projection.ValueList), 2)
			{
				x := s.Projection.ValueList[0]
				assert.Equal(x.Type(), sql.SelectVarCol)
				col := x.(*sql.Col)
				v := col.Value
				assert.Equal(v.Type(), sql.ExprPrimary)
				primary := v.(*sql.Primary)
				assert.True(primary.CanName.IsSettled())
				assert.True(primary.CanName.IsTableColumn())
				assert.Equal(primary.CanName.TableIndex, 0)
				assert.Equal(primary.CanName.ColumnIndex, 1)
			}
		}

		// the f should be resolved, ie as an alias pointed to the correct one
		{
			cond := s.Where.Condition
			assert.Equal(cond.Type(), sql.ExprBinary)
			bin := cond.(*sql.Binary)
			lhs := bin.L
			assert.Equal(lhs.Type(), sql.ExprRef)
			ref := lhs.(*sql.Ref)
			assert.True(ref.CanName.IsExpr())
		}
	}

	{
		s := compAST(
			`
select (t1.$1) as f1,
       (t2.$2 + 200) as f2
from tab("/a/b/c") as t1,
     tab("/a/b/d") as t2
where f2 == f1
`,
		)
		assert.True(s != nil)
		p := newPlan()
		err := p.resolveSymbol(s)
		assert.True(err == nil)

		assert.Equal(
			len(p.tableList),
			2,
		)

		// the t1.$1 should be resolved
		{
			assert.Equal(len(s.Projection.ValueList), 2)
			{
				x := s.Projection.ValueList[0]
				assert.Equal(x.Type(), sql.SelectVarCol)
				col := x.(*sql.Col)
				v := col.Value
				assert.Equal(v.Type(), sql.ExprPrimary)
				primary := v.(*sql.Primary)
				assert.True(primary.CanName.IsSettled())
				assert.True(primary.CanName.IsTableColumn())
				assert.Equal(primary.CanName.TableIndex, 0)
				assert.Equal(primary.CanName.ColumnIndex, 1)
			}

			{
				x := s.Projection.ValueList[1]
				assert.Equal(x.Type(), sql.SelectVarCol)
				col := x.(*sql.Col)
				v := col.Value
				assert.Equal(v.Type(), sql.ExprBinary)
				binary := v.(*sql.Binary)
				lhs := binary.L
				assert.Equal(lhs.Type(), sql.ExprPrimary)
				primary := lhs.(*sql.Primary)
				assert.True(primary.CanName.IsSettled())
				assert.True(primary.CanName.IsTableColumn())
				assert.Equal(primary.CanName.TableIndex, 1)
				assert.Equal(primary.CanName.ColumnIndex, 2)
			}
		}

		// the f should be resolved, ie as an alias pointed to the correct one
		{
			cond := s.Where.Condition
			assert.Equal(cond.Type(), sql.ExprBinary)
			bin := cond.(*sql.Binary)
			{
				lhs := bin.L
				assert.Equal(lhs.Type(), sql.ExprRef)
				ref := lhs.(*sql.Ref)
				assert.True(ref.CanName.IsExpr())
			}

			{
				rhs := bin.R
				assert.Equal(rhs.Type(), sql.ExprRef)
				ref := rhs.(*sql.Ref)
				assert.True(ref.CanName.IsTableColumn())
			}
		}
	}

	{
		s := compAST(
			`
select (t1.$1) as f1,
       (t2.$2 + 200) as f2

from tab("/a/b/c") as t1,
     tab("/a/b/d") as t2

where f2 == t2.$10
`,
		)
		assert.True(s != nil)
		p := newPlan()
		err := p.resolveSymbol(s)
		assert.True(err == nil)

		assert.Equal(
			len(p.tableList),
			2,
		)

		// the t1.$1 should be resolved
		{
			assert.Equal(len(s.Projection.ValueList), 2)
			{
				x := s.Projection.ValueList[0]
				assert.Equal(x.Type(), sql.SelectVarCol)
				col := x.(*sql.Col)
				v := col.Value
				assert.Equal(v.Type(), sql.ExprPrimary)
				primary := v.(*sql.Primary)
				assert.True(primary.CanName.IsSettled())
				assert.True(primary.CanName.IsTableColumn())
				assert.Equal(primary.CanName.TableIndex, 0)
				assert.Equal(primary.CanName.ColumnIndex, 1)
			}

			{
				x := s.Projection.ValueList[1]
				assert.Equal(x.Type(), sql.SelectVarCol)
				col := x.(*sql.Col)
				v := col.Value
				assert.Equal(v.Type(), sql.ExprBinary)
				binary := v.(*sql.Binary)
				lhs := binary.L
				assert.Equal(lhs.Type(), sql.ExprPrimary)
				primary := lhs.(*sql.Primary)
				assert.True(primary.CanName.IsSettled())
				assert.True(primary.CanName.IsTableColumn())
				assert.Equal(primary.CanName.TableIndex, 1)
				assert.Equal(primary.CanName.ColumnIndex, 2)
			}
		}

		// the f should be resolved, ie as an alias pointed to the correct one
		{
			cond := s.Where.Condition
			assert.Equal(cond.Type(), sql.ExprBinary)
			bin := cond.(*sql.Binary)
			{
				lhs := bin.L
				assert.Equal(lhs.Type(), sql.ExprRef)
				ref := lhs.(*sql.Ref)
				assert.True(ref.CanName.IsExpr())
			}

			{
				rhs := bin.R
				assert.Equal(rhs.Type(), sql.ExprPrimary)
				primary := rhs.(*sql.Primary)
				assert.True(primary.CanName.IsTableColumn())
				assert.Equal(primary.CanName.TableIndex, 1)
				assert.Equal(primary.CanName.ColumnIndex, 10)
			}
		}
	}

	{
		s := compAST(
			`
select (t1.$1) as f1,
       (t2.$2 + 200) as f2

from tab("/a/b/c") as t1,
     tab("/a/b/d") as t2

where foo(f1, f2)
`,
		)
		assert.True(s != nil)
		p := newPlan()
		err := p.resolveSymbol(s)
		assert.True(err == nil)

		assert.Equal(
			len(p.tableList),
			2,
		)

		// the t1.$1 should be resolved
		{
			assert.Equal(len(s.Projection.ValueList), 2)
			{
				x := s.Projection.ValueList[0]
				assert.Equal(x.Type(), sql.SelectVarCol)
				col := x.(*sql.Col)
				v := col.Value
				assert.Equal(v.Type(), sql.ExprPrimary)
				primary := v.(*sql.Primary)
				assert.True(primary.CanName.IsSettled())
				assert.True(primary.CanName.IsTableColumn())
				assert.Equal(primary.CanName.TableIndex, 0)
				assert.Equal(primary.CanName.ColumnIndex, 1)
			}

			{
				x := s.Projection.ValueList[1]
				assert.Equal(x.Type(), sql.SelectVarCol)
				col := x.(*sql.Col)
				v := col.Value
				assert.Equal(v.Type(), sql.ExprBinary)
				binary := v.(*sql.Binary)
				lhs := binary.L
				assert.Equal(lhs.Type(), sql.ExprPrimary)
				primary := lhs.(*sql.Primary)
				assert.True(primary.CanName.IsSettled())
				assert.True(primary.CanName.IsTableColumn())
				assert.Equal(primary.CanName.TableIndex, 1)
				assert.Equal(primary.CanName.ColumnIndex, 2)
			}
		}

		// the f should be resolved, ie as an alias pointed to the correct one
		{
			cond := s.Where.Condition
			assert.Equal(cond.Type(), sql.ExprPrimary)
			primary := cond.(*sql.Primary)
			assert.Equal(primary.Leading.Type(), sql.ExprRef)
			assert.True(primary.Leading.(*sql.Ref).CanName.IsFree())
			assert.Equal(len(primary.Suffix), 1)
			{
				suffix := primary.Suffix[0]
				assert.Equal(suffix.Ty, sql.SuffixCall)
				assert.Equal(len(suffix.Call.Parameters), 2)
				{
					p0 := suffix.Call.Parameters[0]
					assert.Equal(p0.Type(), sql.ExprRef)
					ref := p0.(*sql.Ref)
					assert.True(ref.CanName.IsTableColumn())
					assert.Equal(ref.CanName.TableIndex, 0)
					assert.Equal(ref.CanName.ColumnIndex, 1)
				}

				{
					p1 := suffix.Call.Parameters[1]
					assert.Equal(p1.Type(), sql.ExprRef)
					ref := p1.(*sql.Ref)
					assert.True(ref.CanName.IsExpr())
				}
			}
		}
	}

	{
		s := compAST(
			`
select (t1.$1) as f1,
       (t2.$2 + 200) as f2

from tab("/a/b/c") as t1,
     tab("/a/b/d") as t2

where foo(t1.$1)
`,
		)
		assert.True(s != nil)
		p := newPlan()
		err := p.resolveSymbol(s)
		assert.True(err == nil)

		assert.Equal(
			len(p.tableList),
			2,
		)

		// the t1.$1 should be resolved
		{
			assert.Equal(len(s.Projection.ValueList), 2)
			{
				x := s.Projection.ValueList[0]
				assert.Equal(x.Type(), sql.SelectVarCol)
				col := x.(*sql.Col)
				v := col.Value
				assert.Equal(v.Type(), sql.ExprPrimary)
				primary := v.(*sql.Primary)
				assert.True(primary.CanName.IsSettled())
				assert.True(primary.CanName.IsTableColumn())
				assert.Equal(primary.CanName.TableIndex, 0)
				assert.Equal(primary.CanName.ColumnIndex, 1)
			}

			{
				x := s.Projection.ValueList[1]
				assert.Equal(x.Type(), sql.SelectVarCol)
				col := x.(*sql.Col)
				v := col.Value
				assert.Equal(v.Type(), sql.ExprBinary)
				binary := v.(*sql.Binary)
				lhs := binary.L
				assert.Equal(lhs.Type(), sql.ExprPrimary)
				primary := lhs.(*sql.Primary)
				assert.True(primary.CanName.IsSettled())
				assert.True(primary.CanName.IsTableColumn())
				assert.Equal(primary.CanName.TableIndex, 1)
				assert.Equal(primary.CanName.ColumnIndex, 2)
			}
		}

		// the f should be resolved, ie as an alias pointed to the correct one
		{
			cond := s.Where.Condition
			assert.Equal(cond.Type(), sql.ExprPrimary)
			primary := cond.(*sql.Primary)
			assert.Equal(primary.Leading.Type(), sql.ExprRef)
			assert.True(primary.Leading.(*sql.Ref).CanName.IsFree())
			assert.Equal(len(primary.Suffix), 1)
			{
				suffix := primary.Suffix[0]
				assert.Equal(suffix.Ty, sql.SuffixCall)
				assert.Equal(len(suffix.Call.Parameters), 1)
				{
					p0 := suffix.Call.Parameters[0]
					assert.Equal(p0.Type(), sql.ExprPrimary)
					primary := p0.(*sql.Primary)
					assert.True(primary.CanName.IsTableColumn())
					assert.Equal(primary.CanName.TableIndex, 0)
					assert.Equal(primary.CanName.ColumnIndex, 1)
				}
			}
		}
	}
}

func TestCanNameGroupBy(t *testing.T) {
	assert := assert.New(t)
	{
		s := compAST(
			`
select (t1.$1) as f1,
       (t2.$2 + 200) as f2

from tab("/a/b/c") as t1,
     tab("/a/b/d") as t2

group by f1, f2, t2.$3
`,
		)
		assert.True(s != nil)
		p := newPlan()
		err := p.resolveSymbol(s)
		assert.True(err == nil)

		assert.Equal(
			len(p.tableList),
			2,
		)

		// the t1.$1 should be resolved
		{
			assert.Equal(len(s.Projection.ValueList), 2)
			{
				x := s.Projection.ValueList[0]
				assert.Equal(x.Type(), sql.SelectVarCol)
				col := x.(*sql.Col)
				v := col.Value
				assert.Equal(v.Type(), sql.ExprPrimary)
				primary := v.(*sql.Primary)
				assert.True(primary.CanName.IsSettled())
				assert.True(primary.CanName.IsTableColumn())
				assert.Equal(primary.CanName.TableIndex, 0)
				assert.Equal(primary.CanName.ColumnIndex, 1)
			}

			{
				x := s.Projection.ValueList[1]
				assert.Equal(x.Type(), sql.SelectVarCol)
				col := x.(*sql.Col)
				v := col.Value
				assert.Equal(v.Type(), sql.ExprBinary)
				binary := v.(*sql.Binary)
				lhs := binary.L
				assert.Equal(lhs.Type(), sql.ExprPrimary)
				primary := lhs.(*sql.Primary)
				assert.True(primary.CanName.IsSettled())
				assert.True(primary.CanName.IsTableColumn())
				assert.Equal(primary.CanName.TableIndex, 1)
				assert.Equal(primary.CanName.ColumnIndex, 2)
			}
		}

		// the f should be resolved, ie as an alias pointed to the correct one
		{
			assert.Equal(len(s.GroupBy.Name), 3)
			{
				vv := s.GroupBy.Name[0]
				assert.Equal(vv.Type(), sql.ExprRef)
				ref := vv.(*sql.Ref)
				assert.True(ref.CanName.IsTableColumn())
				assert.Equal(ref.CanName.TableIndex, 0)
				assert.Equal(ref.CanName.ColumnIndex, 1)
			}

			{
				vv := s.GroupBy.Name[1]
				assert.Equal(vv.Type(), sql.ExprRef)
				ref := vv.(*sql.Ref)
				assert.True(ref.CanName.IsExpr())
			}

			{
				vv := s.GroupBy.Name[2]
				assert.Equal(vv.Type(), sql.ExprPrimary)
				primary := vv.(*sql.Primary)
				assert.True(primary.CanName.IsTableColumn())
				assert.Equal(primary.CanName.TableIndex, 1)
				assert.Equal(primary.CanName.ColumnIndex, 3)
			}
		}
	}
}

func TestCanNameOrderBy(t *testing.T) {
	assert := assert.New(t)
	{
		s := compAST(
			`
select (t1.$1) as f1,
       (t2.$2 + 200) as f2

from tab("/a/b/c") as t1,
     tab("/a/b/d") as t2

order by f1, f2, t2.$3 asc
`,
		)
		assert.True(s != nil)
		p := newPlan()
		err := p.resolveSymbol(s)
		assert.True(err == nil)

		assert.Equal(
			len(p.tableList),
			2,
		)

		// the t1.$1 should be resolved
		{
			assert.Equal(len(s.Projection.ValueList), 2)
			{
				x := s.Projection.ValueList[0]
				assert.Equal(x.Type(), sql.SelectVarCol)
				col := x.(*sql.Col)
				v := col.Value
				assert.Equal(v.Type(), sql.ExprPrimary)
				primary := v.(*sql.Primary)
				assert.True(primary.CanName.IsSettled())
				assert.True(primary.CanName.IsTableColumn())
				assert.Equal(primary.CanName.TableIndex, 0)
				assert.Equal(primary.CanName.ColumnIndex, 1)
			}

			{
				x := s.Projection.ValueList[1]
				assert.Equal(x.Type(), sql.SelectVarCol)
				col := x.(*sql.Col)
				v := col.Value
				assert.Equal(v.Type(), sql.ExprBinary)
				binary := v.(*sql.Binary)
				lhs := binary.L
				assert.Equal(lhs.Type(), sql.ExprPrimary)
				primary := lhs.(*sql.Primary)
				assert.True(primary.CanName.IsSettled())
				assert.True(primary.CanName.IsTableColumn())
				assert.Equal(primary.CanName.TableIndex, 1)
				assert.Equal(primary.CanName.ColumnIndex, 2)
			}
		}

		// the f should be resolved, ie as an alias pointed to the correct one
		{
			assert.Equal(len(s.OrderBy.Name), 3)
			{
				vv := s.OrderBy.Name[0]
				assert.Equal(vv.Type(), sql.ExprRef)
				ref := vv.(*sql.Ref)
				assert.True(ref.CanName.IsTableColumn())
				assert.Equal(ref.CanName.TableIndex, 0)
				assert.Equal(ref.CanName.ColumnIndex, 1)
			}

			{
				vv := s.OrderBy.Name[1]
				assert.Equal(vv.Type(), sql.ExprRef)
				ref := vv.(*sql.Ref)
				assert.True(ref.CanName.IsExpr())
			}

			{
				vv := s.OrderBy.Name[2]
				assert.Equal(vv.Type(), sql.ExprPrimary)
				primary := vv.(*sql.Primary)
				assert.True(primary.CanName.IsTableColumn())
				assert.Equal(primary.CanName.TableIndex, 1)
				assert.Equal(primary.CanName.ColumnIndex, 3)
			}
		}
	}
}

func TestCanNameHaving(t *testing.T) {
	assert := assert.New(t)
	{
		s := compAST(
			`
select (t1.$1) as f1,
       (t2.$2 + 200) as f2

from tab("/a/b/c") as t1,
     tab("/a/b/d") as t2

having f1
`,
		)
		assert.True(s != nil)
		p := newPlan()
		err := p.resolveSymbol(s)
		assert.True(err == nil)

		assert.Equal(
			len(p.tableList),
			2,
		)

		// the t1.$1 should be resolved
		{
			assert.Equal(len(s.Projection.ValueList), 2)
			{
				x := s.Projection.ValueList[0]
				assert.Equal(x.Type(), sql.SelectVarCol)
				col := x.(*sql.Col)
				v := col.Value
				assert.Equal(v.Type(), sql.ExprPrimary)
				primary := v.(*sql.Primary)
				assert.True(primary.CanName.IsSettled())
				assert.True(primary.CanName.IsTableColumn())
				assert.Equal(primary.CanName.TableIndex, 0)
				assert.Equal(primary.CanName.ColumnIndex, 1)
			}

			{
				x := s.Projection.ValueList[1]
				assert.Equal(x.Type(), sql.SelectVarCol)
				col := x.(*sql.Col)
				v := col.Value
				assert.Equal(v.Type(), sql.ExprBinary)
				binary := v.(*sql.Binary)
				lhs := binary.L
				assert.Equal(lhs.Type(), sql.ExprPrimary)
				primary := lhs.(*sql.Primary)
				assert.True(primary.CanName.IsSettled())
				assert.True(primary.CanName.IsTableColumn())
				assert.Equal(primary.CanName.TableIndex, 1)
				assert.Equal(primary.CanName.ColumnIndex, 2)
			}
		}

		// the f should be resolved, ie as an alias pointed to the correct one
		{
			vv := s.Having.Condition
			assert.Equal(vv.Type(), sql.ExprRef)
			ref := vv.(*sql.Ref)
			assert.True(ref.CanName.IsTableColumn())
			assert.Equal(ref.CanName.TableIndex, 0)
			assert.Equal(ref.CanName.ColumnIndex, 1)
		}
	}

	{
		s := compAST(
			`
select (t1.$1) as f1,
       (t2.$2 + 200) as f2

from tab("/a/b/c") as t1,
     tab("/a/b/d") as t2

having f2
`,
		)
		assert.True(s != nil)
		p := newPlan()
		err := p.resolveSymbol(s)
		assert.True(err == nil)

		assert.Equal(
			len(p.tableList),
			2,
		)

		// the t1.$1 should be resolved
		{
			assert.Equal(len(s.Projection.ValueList), 2)
			{
				x := s.Projection.ValueList[0]
				assert.Equal(x.Type(), sql.SelectVarCol)
				col := x.(*sql.Col)
				v := col.Value
				assert.Equal(v.Type(), sql.ExprPrimary)
				primary := v.(*sql.Primary)
				assert.True(primary.CanName.IsSettled())
				assert.True(primary.CanName.IsTableColumn())
				assert.Equal(primary.CanName.TableIndex, 0)
				assert.Equal(primary.CanName.ColumnIndex, 1)
			}

			{
				x := s.Projection.ValueList[1]
				assert.Equal(x.Type(), sql.SelectVarCol)
				col := x.(*sql.Col)
				v := col.Value
				assert.Equal(v.Type(), sql.ExprBinary)
				binary := v.(*sql.Binary)
				lhs := binary.L
				assert.Equal(lhs.Type(), sql.ExprPrimary)
				primary := lhs.(*sql.Primary)
				assert.True(primary.CanName.IsSettled())
				assert.True(primary.CanName.IsTableColumn())
				assert.Equal(primary.CanName.TableIndex, 1)
				assert.Equal(primary.CanName.ColumnIndex, 2)
			}
		}

		// the f should be resolved, ie as an alias pointed to the correct one
		{
			vv := s.Having.Condition
			assert.Equal(vv.Type(), sql.ExprRef)
			ref := vv.(*sql.Ref)
			assert.True(ref.CanName.IsExpr())
		}
	}

	{
		s := compAST(
			`
select (t1.$1) as f1,
       (t2.$2 + 200) as f2

from tab("/a/b/c") as t1,
     tab("/a/b/d") as t2

having t2.$10
`,
		)
		assert.True(s != nil)
		p := newPlan()
		err := p.resolveSymbol(s)
		assert.True(err == nil)

		assert.Equal(
			len(p.tableList),
			2,
		)

		// the t1.$1 should be resolved
		{
			assert.Equal(len(s.Projection.ValueList), 2)
			{
				x := s.Projection.ValueList[0]
				assert.Equal(x.Type(), sql.SelectVarCol)
				col := x.(*sql.Col)
				v := col.Value
				assert.Equal(v.Type(), sql.ExprPrimary)
				primary := v.(*sql.Primary)
				assert.True(primary.CanName.IsSettled())
				assert.True(primary.CanName.IsTableColumn())
				assert.Equal(primary.CanName.TableIndex, 0)
				assert.Equal(primary.CanName.ColumnIndex, 1)
			}

			{
				x := s.Projection.ValueList[1]
				assert.Equal(x.Type(), sql.SelectVarCol)
				col := x.(*sql.Col)
				v := col.Value
				assert.Equal(v.Type(), sql.ExprBinary)
				binary := v.(*sql.Binary)
				lhs := binary.L
				assert.Equal(lhs.Type(), sql.ExprPrimary)
				primary := lhs.(*sql.Primary)
				assert.True(primary.CanName.IsSettled())
				assert.True(primary.CanName.IsTableColumn())
				assert.Equal(primary.CanName.TableIndex, 1)
				assert.Equal(primary.CanName.ColumnIndex, 2)
			}
		}

		// the f should be resolved, ie as an alias pointed to the correct one
		{
			vv := s.Having.Condition
			assert.Equal(vv.Type(), sql.ExprPrimary)
			ref := vv.(*sql.Primary)
			assert.True(ref.CanName.IsTableColumn())
			assert.Equal(ref.CanName.TableIndex, 1)
			assert.Equal(ref.CanName.ColumnIndex, 10)
		}
	}
}
