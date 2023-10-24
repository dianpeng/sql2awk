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

const (
	stageNA = iota
	stageInProjection
)

type Parser struct {
	L     *Lexer
	stage int // used to notify certain grammar
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

	if self.L.Token == TkSemicolon {
		self.L.Next()
	}
	if self.L.Token != TkEof {
		return nil, self.err("dangling code after parser thinks the statement is finished")
	}
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
	var format *Format

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

LOOP:
	for {
		switch self.L.Token {
		case TkFrom:
			if from != nil {
				return nil, self.err("from cluase has already been specified")
			}

			if n, err := self.parseFrom(); err != nil {
				return nil, err
			} else {
				from = n
			}
			break

		case TkWhere:
			if where != nil {
				return nil, self.err("where clause has already been specified")
			}
			if n, err := self.parseWhere(); err != nil {
				return nil, err
			} else {
				where = n
			}
			break

		case TkGroupBy:
			if groupBy != nil {
				return nil, self.err("group by clause has already been specified")
			}
			if n, err := self.parseGroupBy(); err != nil {
				return nil, err
			} else {
				groupBy = n
			}
			break

		case TkHaving:
			if having != nil {
				return nil, self.err("having clause has already been specified")
			}
			if n, err := self.parseHaving(); err != nil {
				return nil, err
			} else {
				having = n
			}
			break

		case TkOrderBy:
			if orderBy != nil {
				return nil, self.err("order by clause has already been specified")
			}
			if n, err := self.parseOrderBy(); err != nil {
				return nil, err
			} else {
				orderBy = n
			}
			break

		case TkLimit:
			if limit != nil {
				return nil, self.err("limit caluse has already been specified")
			}
			if n, err := self.parseLimit(); err != nil {
				return nil, err
			} else {
				limit = n
			}
			break
		case TkFormat:
			if format != nil {
				return nil, self.err("format clause has already been specified")
			}
			if n, err := self.parseFormat(); err != nil {
				return nil, err
			} else {
				format = n
			}
			break
		default:
			break LOOP
		}
	}
	end := self.posEnd()

	if from == nil {
		return nil, self.err("from clause is not specified")
	}

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
		Format:     format,
	}, nil
}

func (self *Parser) parseFormat() (*Format, error) {
	self.L.Next()
	format := &Format{}

	for {
		key := ""
		idx := -1 // column index, only used when key is "column"

		var val *Const

		if self.L.Token != TkId {
			return nil, self.err("expect a *identifier* to be format option")
		}
		key = self.L.Lexeme.Text
		self.L.Next()

		switch key {
		case "title", "border", "base", "number", "string", "rest", "padding":
			break
		case "column":
			if self.L.Token != TkLPar {
				return nil, self.err(
					"expect a '(index)' after column format option",
				)
			} else {
				self.L.Next()
			}
			if self.L.Token != TkInt {
				return nil, self.err(
					"expect a positive integer to specify column index",
				)
			} else {
				idx = int(self.L.Lexeme.Int)
			}
			if self.L.Next() != TkRPar {
				return nil, self.err(
					"expect a ')' to close index expression for column format option",
				)
			}
			self.L.Next()
			break
		default:
			return nil, self.err(
				"unknown format option",
			)
		}

		if self.L.Token != TkAssign {
			return nil, self.err("expect a '=' to assign a value to format option")
		}
		self.L.Next()
		if c := self.parseConstExpr(); c == nil {
			return nil, self.err("expect a const/literal expression to be format option value")
		} else {
			val = c
		}

		switch key {
		case "title":
			format.Title = val
			break
		case "border":
			format.Border = val
			break
		case "padding":
			format.Padding = val
			break
		case "base":
			format.Base = val
			break
		case "number":
			format.Number = val
			break
		case "string":
			format.String = val
			break
		case "rest":
			format.Rest = val
			break
		default:
			format.Column = append(format.Column, FormatColumn{
				Index: idx,
				Value: val,
			})
			break
		}

		if self.L.Token != TkComma {
			break
		} else {
			self.L.Next()
		}
	}

	return format, nil
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
		self.stage = stageInProjection

		if e, err := self.parseExpr(); err != nil {
			return nil, err
		} else {
			val = e
		}

		self.stage = stageNA

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
				if x.HasStar() {
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

	if self.L.Token == TkRewrite {
		if n, err := self.parseRewrite(); err != nil {
			return nil, err
		} else {
			fromVar.Rewrite = n
		}
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

func (self *Parser) parseRewrite() (*Rewrite, error) {
	start := self.posStart()
	out := &Rewrite{}

	self.L.Next()

	// when list
	for self.L.Token != TkEnd {
		if self.L.Token != TkWhen {
			return nil, self.err("expect a *when* for rewrite list")
		}
		self.L.Next()

		clause := &RewriteClause{}
		clauseStart := self.posStart()

		if expr, err := self.parseExpr(); err != nil {
			return nil, err
		} else {
			clause.When = expr
		}

		if self.L.Token != TkThen {
			return nil, self.err("expect a *then* for yielding the rewritted expression")
		}
		self.L.Next()

		switch self.L.Token {
		case TkNext:
			self.L.Next()
			break
		case TkSet:
			// set key word
			self.L.Next()

			// column expression, or ID's here
			for {
				set := &RewriteSet{}

				if self.L.Token != TkId {
					return nil, self.err("expect a column index, example as $1,$2,...")
				}
				set.Column = self.L.Lexeme.Text

				if self.L.Next() != TkAssign {
					return nil, self.err("expect = here to indicate rewrite expression")
				}
				self.L.Next()
				if expr, err := self.parseExpr(); err != nil {
					return nil, err
				} else {
					set.Value = expr
				}
				clause.Set = append(clause.Set, set)

				// allow multiple set with dangling comma, ie user can write as
				// then set $1=1, $2=2, $3=3 ...
				if self.L.Token == TkComma {
					self.L.Next()
				} else {
					break
				}
			}
			break
		default:
			return nil, self.err("expect a set/next after then")
		}

		if self.L.Token == TkSemicolon {
			self.L.Next()
		}

		clause.CodeInfo = self.currentCodeInfo(clauseStart)
		out.List = append(out.List, clause)
	}

	self.L.Next()
	out.CodeInfo = self.currentCodeInfo(start)
	return out, nil
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

const maxOpPrec = 7
const invalidOpPrec = -1

func (self *Parser) binPrec(tk int) int {
	switch tk {
	case TkOr:
		return 0
	case TkAnd:
		return 1
	case TkIn, TkBetween, TkNot:
		return 2
	case TkEq, TkNe:
		return 3
	case TkLt, TkLe, TkGt, TkGe:
		return 4
	case TkAdd, TkSub:
		return 5
	case TkMul, TkDiv, TkMod:
		return 6
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

func (self *Parser) doParseBinBetweenRHS(
	prec int,
) (Expr, Expr, error) {
	lowerBound, err := self.doParseBin(prec)
	if err != nil {
		return nil, nil, err
	}

	if self.L.Token != TkAnd {
		return nil, nil, self.err("expect AND for BETWEEN operator")
	}
	self.L.Next()

	upperBound, err := self.doParseBin(prec)
	if err != nil {
		return nil, nil, err
	}

	return lowerBound, upperBound, nil
}

func (self *Parser) doParseBinInRHS(
	prec int,
) ([]Expr, error) {
	if self.L.Token != TkLPar {
		return nil, self.err("expect '(' for IN operator's lhs")
	}
	self.L.Next()

	out := []Expr{}

	for self.L.Token != TkRPar {
		if v, err := self.parseExpr(); err != nil {
			return nil, err
		} else {
			out = append(out, v)
		}
		if self.L.Token == TkComma {
			self.L.Next()
		} else if self.L.Token != TkRPar {
			return nil, self.err("expect a ',' or ')' after element in IN's lhs")
		}
	}

	self.L.Next()
	if len(out) == 0 {
		return nil, self.err("IN operator's RHS is an empty set, which is not allowed")
	}
	return out, nil
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
		} else if nextPrec < prec {
			break
		}

		ntk := self.L.Next() // eat the operator token

		if tk == TkNot {
			switch ntk {
			case TkIn:
				tk = tkNotIn
				self.L.Next()
				break

			case TkBetween:
				tk = tkNotBetween
				self.L.Next()
				break
			default:
				return nil, self.err(
					"NOT operator shows up, but expect a suffix operator, " +
						"example like NOT IN, NOT BETWEEN etcd ... ",
				)
			}
		}

		var newNode Expr
		switch tk {
		case TkBetween, tkNotBetween:
			if lower, upper, err := self.doParseBinBetweenRHS(nextPrec + 1); err != nil {
				return nil, err
			} else {
				ge := &Binary{
					Op:       TkGe,
					L:        lhs,
					R:        lower,
					CodeInfo: self.currentCodeInfo(start),
				}

				le := &Binary{
					Op:       TkLe,
					L:        lhs,
					R:        upper,
					CodeInfo: self.currentCodeInfo(start),
				}

				between := &Binary{
					Op:       TkAnd,
					L:        ge,
					R:        le,
					CodeInfo: self.currentCodeInfo(start),
				}

				if tk == TkBetween {
					newNode = between
				} else {
					newNode = &Unary{
						Op:       []int{TkNot},
						Operand:  between,
						CodeInfo: self.currentCodeInfo(start),
					}
				}
			}
			break

		case TkIn, tkNotIn:
			if v, err := self.doParseBinInRHS(nextPrec + 1); err != nil {
				return nil, err
			} else {
				var out Expr

				for _, vv := range v {
					eq := &Binary{
						Op:       TkEq,
						L:        lhs,
						R:        vv,
						CodeInfo: self.currentCodeInfo(start),
					}

					if out == nil {
						out = eq
					} else {
						out = &Binary{
							Op:       TkOr,
							L:        out,
							R:        eq,
							CodeInfo: self.currentCodeInfo(start),
						}
					}
				}

				if out == nil {
					out = &Const{
						Ty:       ConstBool,
						Bool:     false,
						CodeInfo: self.currentCodeInfo(start),
					}
				}

				if tk == tkNotIn {
					newNode = &Unary{
						Op:       []int{TkNot},
						Operand:  out,
						CodeInfo: self.currentCodeInfo(start),
					}
				} else {
					newNode = out
				}
			}
			break

		default:
			if v, err := self.doParseBin(nextPrec + 1); err != nil {
				return nil, err
			} else {
				newNode = &Binary{
					Op:       tk,
					L:        lhs,
					R:        v,
					CodeInfo: self.currentCodeInfo(start),
				}
			}
			break
		}

		lhs = newNode
		start = self.posEnd()
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
			call, err := self.parseSuffixCall(atomic)
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
	symbol := SymbolNone

	switch n {
	case TkId, TkStr:
		id = self.L.Lexeme.Text
		self.L.Next()
		break

	case TkMul, TkColumns, TkRows:
		if self.stage == stageInProjection {
			switch n {
			case TkMul:
				symbol = SymbolStar
				break
			case TkColumns:
				symbol = SymbolColumns
				break
			default:
				symbol = SymbolRows
				break
			}
			self.L.Next()
		} else {
			return nil, self.err("invalid */COLUMNS/ROWS keyword here, must be in projection")
		}
		break

	default:
		return nil, self.err("expect a identifier after '.' operator")
	}

	end := self.posEnd()
	return &Suffix{
		Ty:        SuffixDot,
		Component: id,
		Symbol:    symbol,
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

func (self *Parser) isAggFunc(leading Expr) bool {
	if leading.Type() == ExprRef {
		ref := leading.(*Ref)
		return IsAggFunc(ref.Id)
	}
	return false
}

func (self *Parser) parseSuffixCall(leading Expr) (*Suffix, error) {
	start := self.posStart()

	params := []Expr{}

	if self.L.Next() != TkRPar {
		for self.L.Token != TkRPar {
			var expr Expr

			if self.L.Token == TkMul {
				start := self.posStart()
				self.L.Next()
				if self.isAggFunc(leading) {
					// notes(dpeng):
					// the plan.agg phase will correctly resolve it later on by replacing
					// it with a constant or failed when certain function like min/max/sum
					// which cannot deal with */wildcard parameter
					expr = &Ref{
						CodeInfo: self.currentCodeInfo(start),
						Id:       "*",
					}
				}
			} else {
				if e, err := self.parseExpr(); err != nil {
					return nil, err
				} else {
					expr = e
				}
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

	// unary can also be treated as *constant expression*, but in our grammar
	// it is indeed an unary expression, we need to resolve this here as well
	case TkNot, TkAdd, TkSub:
		uop := []int{self.L.Token}
		self.L.Next()

		// get all the unary operations, this is really constant folding ...
	LOOP:
		for {
			switch self.L.Token {
			case TkNot, TkSub:
				uop = append(uop, self.L.Token)
				self.L.Next()
				break
			case TkAdd:
				self.L.Next()
				break
			default:
				break LOOP
			}
		}

		// try to parse the rest as constant expression
		if cc := self.parseConstExpr(); cc == nil {
			return nil
		} else {
			lUop := len(uop)
			for i := lUop - 1; i >= 0; i-- {
				op := uop[i]
				switch op {
				case TkNot:
					switch cc.Ty {
					case ConstNull:
						cc.Bool = true
						cc.Ty = ConstBool
						break
					case ConstBool:
						cc.Bool = !cc.Bool
						break
					case ConstInt:
						cc.Bool = (cc.Int != 0)
						cc.Ty = ConstBool
						break
					case ConstReal:
						cc.Bool = (cc.Real != 0.0)
						cc.Ty = ConstReal
						break
					default:
						cc.Bool = (len(cc.String) != 0)
						cc.Ty = ConstStr
						break
					}
					break
				default:
					switch cc.Ty {
					case ConstBool:
						if cc.Bool {
							cc.Int = int64(-1)
						} else {
							cc.Int = int64(0)
						}
						cc.Ty = ConstInt
						break
					case ConstInt:
						cc.Int = -cc.Int
						break
					case ConstReal:
						cc.Real = -cc.Real
						break
					default:
						return nil
					}
					break
				}
			}
			return cc
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
	sym := 0

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

	case TkColumns, TkRows:
		if self.stage == stageInProjection {
			if self.L.Token == TkColumns {
				sym = SymbolColumns
			} else {
				sym = SymbolRows
			}
			ty = atomicId
			id = ""
			self.L.Next()
		} else {
			return nil, self.err("COLUMNS/ROWS can only in projection")
		}
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
			Id:     id,
			Symbol: sym,
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
