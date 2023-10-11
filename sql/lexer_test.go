package sql

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestComment(t *testing.T) {
	assert := assert.New(t)
	{
		l := newLexer(`
// last line
#  last line
`)
		assert.True(l.Next() == TkEof)
	}

	{
		l := newLexer(`
#  last line
// last line
`)
		assert.True(l.Next() == TkEof)
	}

	{
		l := newLexer(`
# abc
    id #def
# xyz
`)
		assert.True(l.Next() == TkId)
		assert.True(l.Lexeme.Text == "id")
		assert.True(l.Next() == TkEof)
	}

	{
		l := newLexer(`
# abc
/* abcd */    id #def
# xyz
`)
		assert.True(l.Next() == TkId)
		assert.True(l.Lexeme.Text == "id")
		assert.True(l.Next() == TkEof)
	}
}

func TestOp(t *testing.T) {
	assert := assert.New(t)
	{
		l := newLexer("+-*/%.[]{}():;::?")
		assert.True(l.Next() == TkAdd)
		assert.True(l.Next() == TkSub)
		assert.True(l.Next() == TkMul)
		assert.True(l.Next() == TkDiv)
		assert.True(l.Next() == TkMod)
		assert.True(l.Next() == TkDot)
		assert.True(l.Next() == TkLSqr)
		assert.True(l.Next() == TkRSqr)
		assert.True(l.Next() == TkLBra)
		assert.True(l.Next() == TkRBra)
		assert.True(l.Next() == TkLPar)
		assert.True(l.Next() == TkRPar)
		assert.True(l.Next() == TkColon)
		assert.True(l.Next() == TkSemicolon)
		assert.True(l.Next() == TkDColon)
		assert.True(l.Next() == TkQuestion)
		assert.True(l.Next() == TkEof)
	}

	{
		l := newLexer(">>=<<=!===!!=")
		assert.True(l.Next() == TkGt)
		assert.True(l.Next() == TkGe)
		assert.True(l.Next() == TkLt)
		assert.True(l.Next() == TkLe)
		assert.True(l.Next() == TkNe)
		assert.True(l.Next() == TkEq)
		assert.True(l.Next() == TkNot)
		assert.True(l.Next() == TkNe)
		assert.True(l.Next() == TkEof)
	}

	{
		l := newLexer("&&||!")
		assert.True(l.Next() == TkAnd)
		assert.True(l.Next() == TkOr)
		assert.True(l.Next() == TkNot)
		assert.True(l.Next() == TkEof)
	}
}

func TestId(t *testing.T) {
	assert := assert.New(t)
	{
		l := newLexer("a")
		assert.True(l.Next() == TkId)
		assert.Equal(l.Lexeme.Text, "a", "id == a")
	}
}

func TestNumber(t *testing.T) {
	assert := assert.New(t)
	{
		l := newLexer("0x1")
		assert.True(l.Next() == TkInt)
		assert.Equal(l.Lexeme.Int, int64(0x1), "num == 0x1")
	}
	{
		l := newLexer("123")
		assert.True(l.Next() == TkInt)
		assert.Equal(l.Lexeme.Int, int64(123), "num == 123")
	}
	{
		l := newLexer("1.23")
		assert.True(l.Next() == TkReal)
		assert.Equal(l.Lexeme.Real, float64(1.23), "num == 1.23")
	}
	{
		l := newLexer("1.0e2")
		assert.True(l.Next() == TkReal)
		assert.Equal(l.Lexeme.Real, float64(1.0e2), "num == 1e2")
	}
}

func TestKeyword(t *testing.T) {
	assert := assert.New(t)
	{
		l := newLexer("TRue True_")
		assert.True(l.Next() == TkTrue)
		assert.True(l.Next() == TkId)
		assert.Equal(l.Lexeme.Text, "true_", "true_")
	}

	{
		l := newLexer("faLSE falsE_")
		assert.True(l.Next() == TkFalse)
		assert.True(l.Next() == TkId)
		assert.Equal(l.Lexeme.Text, "false_", "false_")
	}

	{
		l := newLexer("NUll nuLl_")
		assert.True(l.Next() == TkNull)
		assert.True(l.Next() == TkId)
		assert.Equal(l.Lexeme.Text, "null_", "null_")
	}

	{
		l := newLexer("select SELECT sELEct")
		assert.True(l.Next() == TkSelect)
		assert.True(l.Next() == TkSelect)
		assert.True(l.Next() == TkSelect)
	}

	{
		l := newLexer("from FROM frOM")
		assert.True(l.Next() == TkFrom)
		assert.True(l.Next() == TkFrom)
		assert.True(l.Next() == TkFrom)
	}

	{
		l := newLexer("AS as As")
		assert.True(l.Next() == TkAs)
		assert.True(l.Next() == TkAs)
		assert.True(l.Next() == TkAs)
	}

	{
		l := newLexer("CAST cast CaST")
		assert.True(l.Next() == TkCast)
		assert.True(l.Next() == TkCast)
		assert.True(l.Next() == TkCast)
	}

	{
		l := newLexer("where WHERE WHere")
		assert.True(l.Next() == TkWhere)
		assert.True(l.Next() == TkWhere)
		assert.True(l.Next() == TkWhere)
	}

	{
		l := newLexer("having HAVING haVING")
		assert.True(l.Next() == TkHaving)
		assert.True(l.Next() == TkHaving)
		assert.True(l.Next() == TkHaving)
	}

	{
		l := newLexer("limit LIMIT LImit")
		assert.True(l.Next() == TkLimit)
		assert.True(l.Next() == TkLimit)
		assert.True(l.Next() == TkLimit)
	}

	{
		l := newLexer("distinct DISTINCT disTINCT")
		assert.True(l.Next() == TkDistinct)
		assert.True(l.Next() == TkDistinct)
		assert.True(l.Next() == TkDistinct)
	}

	{
		l := newLexer("GROUP by group by grouP By")
		assert.True(l.Next() == TkGroupBy)
		assert.True(l.Next() == TkGroupBy)
		assert.True(l.Next() == TkGroupBy)
	}

	{
		l := newLexer("order by ORDER       by ordeR By")
		assert.True(l.Next() == TkOrderBy)
		assert.Equal(l.Next(), TkOrderBy)
		assert.Equal(l.Next(), TkOrderBy)
	}
}

func TestString(t *testing.T) {
	assert := assert.New(t)
	{
		l := newLexer("''")
		assert.True(l.Next() == TkStr)
		assert.Equal(l.Lexeme.Text, "", "str == ''")
	}

	{
		l := newLexer("'a'")
		assert.True(l.Next() == TkStr)
		assert.Equal(l.Lexeme.Text, "a", "str == 'a'")
	}
	{
		l := newLexer("'key'")
		assert.True(l.Next() == TkStr)
		assert.Equal(l.Lexeme.Text, "key", "str == 'key'")
	}

	{
		l := newLexer("\"\"")
		assert.True(l.Next() == TkStr)
		assert.Equal(l.Lexeme.Text, "", "str == ''")
	}

	{
		l := newLexer("'\\t'")
		assert.True(l.Next() == TkStr)
		assert.Equal(l.Lexeme.Text, "\t", "str == '\\t'")
	}

	{
		l := newLexer("'\\n'")
		assert.True(l.Next() == TkStr)
		assert.Equal(l.Lexeme.Text, "\n", "str == '\\n'")
	}

	{
		l := newLexer("'\\b'")
		assert.True(l.Next() == TkStr)
		assert.Equal(l.Lexeme.Text, "\b", "str == '\\b'")
	}

	{
		l := newLexer("'\\v'")
		assert.True(l.Next() == TkStr)
		assert.Equal(l.Lexeme.Text, "\v", "str == '\\v'")
	}

	{
		l := newLexer("'\\r'")
		assert.True(l.Next() == TkStr)
		assert.Equal(l.Lexeme.Text, "\r", "str == '\\r'")
	}

	{
		l := newLexer("'\\''")
		assert.True(l.Next() == TkStr)
		assert.Equal(l.Lexeme.Text, "'", "str == '''")
	}

	{
		l := newLexer("'\\\"'")
		assert.True(l.Next() == TkStr)
		assert.Equal(l.Lexeme.Text, "\"", "str == '\"'")
	}
}
