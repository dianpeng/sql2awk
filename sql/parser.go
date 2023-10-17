package sql

// parser of the sql, which is tailered for our own usage. We briefly describe
// the grammar of sql as following EBNF
//
// ### statement -------------------------------------------------------------
//
// code := select
// select :=
//     SELECT projection
//     from?
//     where?
//     group-by?
//     having?
//     order-by?
//     limit?
//
// col-name := ID ('.' ID)
// col-name-list := col-name (',' col-name)*
//
// projection :=
//   STAR |
//   project-var+
// project-var := DISTINCT? agg? expr as?
// agg := MIN | MAX | AVG | SUM | COUNT
// as := [AS ID]?
//
// from := FROM from-var-list?
// from-var-list := from-var+
// from-var := ID '(' from-var-arg-list? ')'
// from-var-arg-list := from-var-arg (',' from-var-arg)*
// from-var-arg := const
//
// where := WHERE expr
//
// group-by := GROUPBY col-name-list?
//
// having := HAVING expr
//
// order-by := ORDERBY (order-by-opt)?
// order-by-opt := 'ASC' | 'DESC'
//
// limit := LIMIT INT
//
// ### expression -------------------------------------------------------------
// expr :=
//   ternary |
//   binary  |
//   unary   |
//   primary |
//   suffix  |
//   const
//
// ternary := expr '?' expr ':' expr
//
// binary := expr binary-op binary
// binary-op := ...
//
// unary := unary-op+ expr
// unary-op := ...
//
// primary := '(' expr ')'
//
// suffix := expr suffix-component-list?
// suffix-component-list := (index | dot | call)+
// index := '[' expr ']'
// dot := '.' (ID|STR)
// call := '(' call-arg-list? ')'
// call-arg-list := expr (',' expr)*
//
// const := INT | FLOAT | TRUE | FALSE | NULL | STR
//
// ----------------------------------------------------------------------------

import (
	"fmt"
)

const (
	atomicConst = iota
	atomicId
	atomicExpr
)

type Parser struct {
	L *Lexer
}

func newParser(xx string) *Parser {
	return &Parser{
		L: newLexer(xx),
	}
}

func NewParser(xx string) *Parser {
	return newParser(xx)
}

func (self *Parser) posStart() int {
	return self.L.Cursor
}

func (self *Parser) posEnd() int {
	return self.L.Cursor
}

func (self *Parser) snippet(start, end int) string {
	if start >= end {
		start = end
	}
	return self.L.Source[start:end]
}

func (self *Parser) err(msg string) error {
	if self.L.Token == TkError {
		return fmt.Errorf("%s", self.L.Lexeme.Text)
	} else {
		return fmt.Errorf("%s: %s", self.L.dinfo(), msg)
	}
}

func (self *Parser) expect(tk int) error {
	if self.L.Token == tk {
		self.L.Next()
		return nil
	} else {
		return self.err("unexpected token during grammar parsing")
	}
}

func (self *Parser) currentCodeInfo(start int) CodeInfo {
	return CodeInfo{
		Start:   start,
		End:     self.posEnd(),
		Snippet: self.snippet(start, self.posEnd()),
	}
}

func (self *Parser) Parse() (*Code, error) {
	c := &Code{}
	start := self.posStart()

	self.L.Next()
	switch self.L.Token {
	case TkSelect:
		if n, err := self.parseSelect(); err != nil {
			return nil, err
		} else {
			c.Select = n
		}
		break
	default:
		return nil, self.err("unknown statement, expect *select*")
	}

	c.CodeInfo = self.currentCodeInfo(start)
	return c, nil
}

func (self *Parser) parseSelect() (*Select, error) {
	self.L.Next() // skip the *select* keyword

	var projection *Projection
	var from *From
	var where *Where
	var groupBy *GroupBy
	var having *Having
	var orderBy *OrderBy
	var limit *Limit

	distinct := false

	start := self.posStart()

	if self.L.Token == TkDistinct {
		distinct = true
		self.L.Next()
	}

	// projection
	if n, err := self.parseProjection(); err != nil {
		return nil, err
	} else {
		projection = n
	}

	// from
	if self.L.Token != TkFrom {
		return nil, self.err("expect *from* here to specify the table")
	}
	if n, err := self.parseFrom(); err != nil {
		return nil, err
	} else {
		from = n
	}

	// where clause, optional
	if self.L.Token == TkWhere {
		if n, err := self.parseWhere(); err != nil {
			return nil, err
		} else {
			where = n
		}
	}

	// group by, optional
	if self.L.Token == TkGroupBy {
		if n, err := self.parseGroupBy(); err != nil {
			return nil, err
		} else {
			groupBy = n
		}
	}

	// having, optional
	if self.L.Token == TkHaving {
		if n, err := self.parseHaving(); err != nil {
			return nil, err
		} else {
			having = n
		}
	}

	// order by, optional
	if self.L.Token == TkOrderBy {
		if n, err := self.parseOrderBy(); err != nil {
			return nil, err
		} else {
			orderBy = n
		}
	}

	// limit, optional
	if self.L.Token == TkLimit {
		if n, err := self.parseLimit(); err != nil {
			return nil, err
		} else {
			limit = n
		}
	}

	end := self.posEnd()

	return &Select{
		CodeInfo: CodeInfo{
			Start:   start,
			End:     end,
			Snippet: self.snippet(start, end),
		},
		Distinct:   distinct,
		Projection: projection,
		From:       from,
		Where:      where,
		GroupBy:    groupBy,
		Having:     having,
		OrderBy:    orderBy,
		Limit:      limit,
	}, nil
}

func (self *Parser) parseProjectionVar(idx int) (SelectVar, error) {
	start := self.posStart()

	switch self.L.Token {
	case TkMul: // star
		self.L.Next()
		return &Star{
			CodeInfo: self.currentCodeInfo(start),
		}, nil

	default:
		var val Expr
		alias := ""

		if e, err := self.parseExpr(); err != nil {
			return nil, err
		} else {
			val = e
		}

		if self.L.Token == TkAs {
			if self.L.Next() != TkId {
				return nil, self.err("expect an alias identifier after *as*")
			}
			alias = self.L.Lexeme.Text
			self.L.Next()
		}

		return &Col{
			CodeInfo: self.currentCodeInfo(start),
			ColIndex: idx,
			As:       alias,
			Value:    val,
		}, nil
	}
}

// SQLLIST, which is a name I coin to represent grammar like following :
// element (',' element)*, the difference between the normal one is that the
// list will never be empty.  This sort of list is kind of stupid, since we need
// to at least expect one from the vars, and afterwards, we expect another one
// *after* a ',' here.  this is same for *projection*, *from*, *order by*

func (self *Parser) parseSqlList(
	visitor func(int) error,
) error {
	if err := visitor(0); err != nil {
		return err
	}
	idx := 1

	for {
		if self.L.Token != TkComma {
			break
		}
		self.L.Next()
		if err := visitor(idx); err != nil {
			return err
		}
		idx++
	}

	return nil
}

func (self *Parser) parseProjection() (*Projection, error) {
	x := SelectVarList{}
	start := self.posStart()

	if err := self.parseSqlList(
		func(idx int) error {
			if n, err := self.parseProjectionVar(idx); err != nil {
				return err
			} else {
				if x.HasWildcard() {
					return self.err("duplicated */wildcard specified")
				}
				x = append(x, n)
			}
			return nil
		},
	); err != nil {
		return nil, err
	}

	return &Projection{
		CodeInfo:  self.currentCodeInfo(start),
		ValueList: x,
	}, nil
}

func (self *Parser) parseFromVar() (*FromVar, error) {
	fromVar := &FromVar{}

	if self.L.Token != TkId {
		return nil, self.err("expect a valid identifier to represent how to load table")
	}

	fromVar.Name = self.L.Lexeme.Text
	self.L.Next()

	// okay, this is just a list of arguments
	if self.L.Token != TkLPar {
		return nil, self.err("expect a '(' here for table locator")
	}
	self.L.Next()

	for self.L.Token != TkRPar {
		if n := self.parseConstExpr(); n == nil {
			return nil, self.err("expect a valid constant to be part of the table locator parameters")
		} else {
			fromVar.Vars = append(fromVar.Vars, n)
		}
		if self.L.Token == TkComma {
			self.L.Next()
		}
	}
	self.L.Next()

	// optional alias
	if self.L.Token == TkAs {
		if self.L.Next() != TkId {
			return nil, self.err("expect a identifier after *as*")
		}
		fromVar.Alias = self.L.Lexeme.Text
		self.L.Next()
	}

	return fromVar, nil
}

func (self *Parser) parseFrom() (*From, error) {
	from := &From{}
	start := self.posStart()

	self.L.Next() // eat the *from*

	if err := self.parseSqlList(
		func(idx int) error {
			if n, err := self.parseFromVar(); err != nil {
				return err
			} else {
				from.VarList = append(from.VarList, n)
			}
			return nil
		},
	); err != nil {
		return nil, err
	}

	from.CodeInfo = self.currentCodeInfo(start)
	return from, nil
}

func (self *Parser) parseWhere() (*Where, error) {
	start := self.posStart()

	self.L.Next()
	if n, err := self.parseExpr(); err != nil {
		return nil, err
	} else {
		return &Where{
			CodeInfo:  self.currentCodeInfo(start),
			Condition: n,
		}, nil
	}
}

func (self *Parser) parseGroupBy() (*GroupBy, error) {
	gb := &GroupBy{}
	start := self.posStart()

	self.L.Next() // eat group by

	if err := self.parseSqlList(
		func(idx int) error {
			if c, err := self.parseExpr(); err != nil {
				return err
			} else {
				gb.Name = append(gb.Name, c)
			}
			return nil
		},
	); err != nil {
		return nil, err
	}

	gb.CodeInfo = self.currentCodeInfo(start)
	return gb, nil
}

func (self *Parser) parseHaving() (*Having, error) {
	if x, err := self.parseWhere(); err != nil {
		return nil, err
	} else {
		return (*Having)(x), nil
	}
}

func (self *Parser) parseOrderBy() (*OrderBy, error) {
	oB := &OrderBy{
		Order: OrderAsc,
	}
	start := self.posStart()
	self.L.Next() // eat order by

	// list of expression to be used for sorting keys

	if err := self.parseSqlList(
		func(idx int) error {
			if c, err := self.parseExpr(); err != nil {
				return err
			} else {
				oB.Name = append(oB.Name, c)
			}
			return nil
		},
	); err != nil {
		return nil, err
	}

	oB.Order = OrderAsc

	if self.L.Token == TkId {
		switch self.L.lowerText() {
		case "asc":
			oB.Order = OrderAsc
			break

		case "desc":
			oB.Order = OrderDesc
			break
		}
		self.L.Next()
	}

	oB.CodeInfo = self.currentCodeInfo(start)
	return oB, nil
}

func (self *Parser) parseLimit() (*Limit, error) {
	limit := &Limit{}

	if self.L.Next() != TkInt {
		return nil, self.err("expect a integer after limit")
	}
	limit.Limit = self.L.Lexeme.Int
	self.L.Next()
	return limit, nil
}

func (self *Parser) parseColName() (*ColName, error) {
	t := ""
	n := ""

	if self.L.Token != TkId {
		return nil, self.err(
			"expect a identifier to represent column name",
		)
	}

	n = self.L.Lexeme.Text
	self.L.Next()

	if self.L.Token == TkDot {
		if self.L.Next() != TkId {
			return nil, self.err(
				"expect a identifier to represent column name after table name",
			)
		}
		t = n
		n = self.L.Lexeme.Text
		self.L.Next()
	}

	return &ColName{
		Table: t,
		Name:  n,
	}, nil
}

// ----------------------------------------------------------------------------
// Expression Parsing
// ----------------------------------------------------------------------------

func (self *Parser) parseExpr() (Expr, error) {
	return self.parseTernary()
}

func (self *Parser) parseTernary() (Expr, error) {
	start := self.posStart()

	cond, err := self.parseBinary()
	if err != nil {
		return nil, err
	}

	// check whether we have a ? mark
	if self.L.Token == TkQuestion {
		self.L.Next()

		l, err := self.parseBinary()

		if err != nil {
			return nil, err
		}

		if err := self.expect(TkColon); err != nil {
			return nil, err
		}

		r, err := self.parseBinary()
		if err != nil {
			return nil, err
		}

		end := self.posEnd()

		return &Ternary{
			Cond: cond,
			B0:   l,
			B1:   r,
			CodeInfo: CodeInfo{
				Start:   start,
				End:     end,
				Snippet: self.snippet(start, end),
			},
		}, nil
	}
	return cond, nil
}

const maxOpPrec = 6
const invalidOpPrec = -1

func (self *Parser) binPrec(tk int) int {
	switch tk {
	case TkOr:
		return 0
	case TkAnd:
		return 1
	case TkEq, TkNe:
		return 2
	case TkLt, TkLe, TkGt, TkGe:
		return 3
	case TkAdd, TkSub:
		return 4
	case TkMul, TkDiv, TkMod:
		return 5
	default:
		return invalidOpPrec
	}
}

// Binary parsing, precedence climbing
func (self *Parser) doParseBin(prec int) (Expr, error) {
	if prec == maxOpPrec {
		return self.parseUnary()
	}

	start := self.posStart()

	l, err := self.parseUnary()
	if err != nil {
		return nil, err
	}

	return self.doParseBinRest(l, prec, start)
}

func (self *Parser) parseBinary() (Expr, error) {
	return self.doParseBin(0)
}

func (self *Parser) doParseBinRest(lhs Expr,
	prec int,
	start int,
) (Expr, error) {

	for {
		tk := self.L.Token
		nextPrec := self.binPrec(tk)

		if nextPrec == invalidOpPrec {
			break
		}

		if nextPrec < prec {
			break
		}

		self.L.Next() // eat the operator token

		rhs, err := self.doParseBin(nextPrec + 1)
		if err != nil {
			return nil, err
		}

		end := self.posEnd()

		newNode := &Binary{
			Op: tk,
			L:  lhs,
			R:  rhs,
			CodeInfo: CodeInfo{
				Start:   start,
				End:     end,
				Snippet: self.snippet(start, end),
			},
		}

		lhs = newNode
		start = end
	}

	return lhs, nil
}

func (self *Parser) parseUnary() (Expr, error) {
	opList := []int{}

	start := self.posStart()

	for {
		cur := self.L.Token
		if cur == TkAdd || cur == TkSub || cur == TkNot {
			opList = append(opList, cur)
			self.L.Next()
		} else {
			break
		}
	}

	expr, err := self.parsePrimary()
	if err != nil {
		return nil, err
	}

	end := self.posEnd()

	if len(opList) > 0 {
		return &Unary{
			Op:      opList,
			Operand: expr,
			CodeInfo: CodeInfo{
				Start:   start,
				End:     end,
				Snippet: self.snippet(start, end),
			},
		}, nil
	} else {
		return expr, nil
	}
}

func (self *Parser) parsePrimary() (Expr, error) {
	start := self.posStart()

	atomic, err := self.parseAtomic()
	if err != nil {
		return nil, err
	}

	suffix := []*Suffix{}

	// check whether we have suffix afterwards
loop:
	for {
		tk := self.L.Token
		switch tk {
		case TkDot:
			dot, err := self.parseSuffixDot()
			if err != nil {
				return nil, err
			}
			suffix = append(suffix, dot)
			break

		case TkLSqr:
			index, err := self.parseSuffixIndex()
			if err != nil {
				return nil, err
			}
			suffix = append(suffix, index)
			break

		case TkLPar:
			call, err := self.parseSuffixCall()
			if err != nil {
				return nil, err
			}
			suffix = append(suffix, call)
			break

		default:
			break loop
		}
	}

	end := self.posEnd()

	if len(suffix) > 0 {
		return &Primary{
			Leading: atomic,
			Suffix:  suffix,
			CodeInfo: CodeInfo{
				Start:   start,
				End:     end,
				Snippet: self.snippet(start, end),
			},
		}, nil
	} else {
		return atomic, nil
	}
}

func (self *Parser) parseSuffixDot() (*Suffix, error) {
	start := self.posStart()
	n := self.L.Next()
	id := ""

	switch n {
	case TkId, TkStr:
		id = self.L.Lexeme.Text
		self.L.Next()
		break
	default:
		return nil, self.err("expect a identifier after '.' operator")
	}

	end := self.posEnd()
	return &Suffix{
		Ty:        SuffixDot,
		Component: id,
		CodeInfo: CodeInfo{
			Start:   start,
			End:     end,
			Snippet: self.snippet(start, end),
		},
	}, nil
}

func (self *Parser) parseSuffixIndex() (*Suffix, error) {
	start := self.posStart()
	self.L.Next()

	expr, err := self.parseExpr()
	if err != nil {
		return nil, err
	}
	if err := self.expect(TkRSqr); err != nil {
		return nil, err
	}

	end := self.posEnd()

	return &Suffix{
		Ty:    SuffixIndex,
		Index: expr,
		CodeInfo: CodeInfo{
			Start:   start,
			End:     end,
			Snippet: self.snippet(start, end),
		},
	}, nil
}

func (self *Parser) parseSuffixCall() (*Suffix, error) {
	start := self.posStart()

	params := []Expr{}

	if self.L.Next() != TkRPar {
		for self.L.Token != TkRPar {
			expr, err := self.parseExpr()
			if err != nil {
				return nil, err
			}
			params = append(params, expr)
			if self.L.Token == TkComma {
				self.L.Next()
			}
		}
		self.L.Next()
	} else {
		self.L.Next()
	}

	end := self.posEnd()

	return &Suffix{
		Ty: SuffixCall,
		Call: &Call{
			Parameters: params,
			CodeInfo: CodeInfo{
				Start:   start,
				End:     end,
				Snippet: self.snippet(start, end),
			},
		},
		CodeInfo: CodeInfo{
			Start:   start,
			End:     end,
			Snippet: self.snippet(start, end),
		},
	}, nil
}

func (self *Parser) parseConstExpr() *Const {
	start := self.posStart()

	switch self.L.Token {
	case TkTrue, TkFalse:
		booleanVal := false
		if self.L.Token == TkTrue {
			booleanVal = true
		} else {
			booleanVal = false
		}
		self.L.Next()
		return &Const{
			Ty:       ConstBool,
			Bool:     booleanVal,
			CodeInfo: self.currentCodeInfo(start),
		}

	case TkNull:
		self.L.Next()
		return &Const{
			Ty:       ConstNull,
			CodeInfo: self.currentCodeInfo(start),
		}

	case TkStr:
		str := self.L.Lexeme.Text
		self.L.Next()
		return &Const{
			Ty:       ConstStr,
			String:   str,
			CodeInfo: self.currentCodeInfo(start),
		}

	case TkInt:
		v := self.L.Lexeme.Int
		self.L.Next()
		return &Const{
			Ty:       ConstInt,
			Int:      v,
			CodeInfo: self.currentCodeInfo(start),
		}

	case TkReal:
		v := self.L.Lexeme.Real
		self.L.Next()
		return &Const{
			Ty:       ConstReal,
			Real:     v,
			CodeInfo: self.currentCodeInfo(start),
		}

	default:
		return nil
	}
}

func (self *Parser) parseAtomic() (Expr, error) {
	var c *Const
	var id string
	var expr Expr
	var ty int

	start := self.posStart()

	switch self.L.Token {
	// =======================================================================
	// Const value

	case TkTrue, TkFalse, TkNull, TkStr, TkInt, TkReal:
		ty = atomicConst
		c = self.parseConstExpr()
		if c == nil {
			panic("unreachable")
		}
		break

	case TkId:
		// notes for symbol of *aggregation* function, we also treat them as ref id
		// though they are keywords under certain context
		ty = atomicId
		id = self.L.Lexeme.Text
		self.L.Next()
		break

	case TkLPar:
		self.L.Next()
		e, err := self.parseExpr()
		if err != nil {
			return nil, err
		}
		if err := self.expect(TkRPar); err != nil {
			return nil, err
		}
		expr = e
		ty = atomicExpr
		break

	default:
		return nil, self.err("unexpected token for expression")
	}

	end := self.posEnd()

	switch ty {
	case atomicConst:
		return c, nil

	case atomicId:
		return &Ref{
			Id: id,
			CodeInfo: CodeInfo{
				Start:   start,
				End:     end,
				Snippet: self.snippet(start, end),
			},
		}, nil

	case atomicExpr:
		return expr, nil

	default:
		panic("unreachable")
		return nil, nil
	}
}
