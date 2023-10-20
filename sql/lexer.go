package sql

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	// Literal
	TkTrue = iota
	TkFalse
	TkNumber
	TkInt
	TkReal
	TkNull
	TkStr
	TkId

	// Keywords
	TkSelect
	TkFrom
	TkAs
	TkCast
	TkWhere
	TkGroupBy
	TkOrderBy
	TkLimit
	TkHaving
	TkDistinct
	TkIn
	TkBetween
	TkDefault
	TkCase
	TkIf
	TkElse
	TkThen
	TkEnd
	TkFormat
	TkRewrite
	TkOutput

	// Punctuation
	TkComma
	TkSemicolon
	TkColon
	TkDColon
	TkQuestion
	TkAssign

	TkLSqr
	TkRSqr
	TkLBra
	TkRBra
	TkLPar
	TkRPar

	TkAdd
	TkSub
	TkMul
	TkDiv
	TkMod

	TkLt
	TkLe
	TkGt
	TkGe
	TkEq
	TkNe

	TkAnd
	TkOr
	TkNot

	TkDot

	TkError
	TkEof

	// Special hidden tokens that will never showsup during lexing, used inside
	// of parser for preprocessing/desugar purpose
	tkNotBetween
	tkNotIn
)

type Lexeme struct {
	Text string
	Int  int64
	Real float64
	Bool bool
}

type Lexer struct {
	Source string
	Cursor int
	Token  int
	Lexeme Lexeme
}

func (self *Lexer) nextRune() (rune, int) {
	if self.Cursor == len(self.Source) {
		return utf8.RuneError, 0
	}
	return utf8.DecodeRuneInString(self.Source[self.Cursor:])
}

func (self *Lexer) nextRune2() rune {
	r, _ := utf8.DecodeRuneInString(self.Source[self.Cursor+1:])
	return r
}

func (self *Lexer) yield(tk int, sz int) int {
	self.Token = tk
	self.Cursor += sz
	return tk
}

func (self *Lexer) eof() int {
	self.Token = TkEof
	return TkEof
}

// generate a debug position for diagnostic information output
func (self *Lexer) pos(where int, source string) (int, int) {
	line := 1
	col := 1
	idx := 0

	for idx < where {
		r, _ := utf8.DecodeRuneInString(source[idx:])
		if r == '\n' {
			line++
			col = 1
		}
		idx++
		col++
	}

	return line, col
}

func (self *Lexer) dinfo() string {
	line, col := self.pos(self.Cursor, self.Source)
	return fmt.Sprintf("around position(%d: %d)", line, col)
}

func (self *Lexer) err(msg string) int {
	self.Lexeme.Text = fmt.Sprintf("%s: %s", self.dinfo(), msg)
	self.Token = TkError
	return TkError
}

func (self *Lexer) errE(err error) int {
	self.Lexeme.Text = fmt.Sprintf("%s: %s", self.dinfo(), err)
	self.Token = TkError
	return TkError
}

func (self *Lexer) errUtf8() int {
	return self.err("invalid utf8 character")
}

func (self *Lexer) lexLineComment() bool {
	for {
		r, sz := utf8.DecodeRuneInString(self.Source[self.Cursor:])
		if r == utf8.RuneError {
			if sz == 0 {
				return true // last line break, ie reaching end of the file
			} else {
				self.errUtf8()
				return false
			}
		}

		self.Cursor += sz

		if r == '\n' {
			break
		}
	}

	return true
}

func (self *Lexer) lexBlockComment() bool {
	for {
		r, sz := utf8.DecodeRuneInString(self.Source[self.Cursor:])
		if r == utf8.RuneError {
			if sz == 0 {
				return true // last line break, ie reaching end of the file
			} else {
				self.errUtf8()
				return false
			}
		}

		if r == '*' {
			rr, ll := utf8.DecodeRuneInString(self.Source[self.Cursor+1:])
			if rr == utf8.RuneError {
				if ll == 0 {
					self.err("block comment is not closed properly")
				} else {
					self.errUtf8()
				}
				return false
			}
			if rr == '/' {
				// end of the comment
				self.Cursor += 2
				break
			}
		}

		self.Cursor += sz
	}

	return true
}

// 1) all the exponential sign indicates to be a real number
// 2) 0x prefix is allowed
// 3) dot digit indicates a real number
// 4) otherwise treated as 64 bits number

func (self *Lexer) lexNum(c rune) int {
	hasDot := false
	hasE := false
	hexSign := false

	buf := &bytes.Buffer{}

	buf.WriteRune(c)

	self.Cursor++ // skip first rune

	if c == '0' {
		r, sz := self.nextRune()

		if r == utf8.RuneError {
			if sz == 0 {
				goto done
			} else {
				return self.errUtf8()
			}
		}
		if r == 'x' || r == 'X' {
			hexSign = true
			buf.WriteRune(c)
			self.Cursor++
		}
	}

loop:
	for {
		r, sz := self.nextRune()
		if r == utf8.RuneError {
			if sz == 0 {
				break
			} else {
				return self.errUtf8()
			}
		}

		switch r {
		case '.':
			if hexSign || hasDot {
				goto done
			}
			buf.WriteRune('.')
			hasDot = true
			break

		case 'e', 'E':
			if hexSign || hasE {
				goto done
			}
			buf.WriteRune(r)
			hasE = true
			break

		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			buf.WriteRune(r)
			break

		default:
			break loop
		}

		self.Cursor += sz
	}

done:
	if hasDot {
		f, err := strconv.ParseFloat(buf.String(), 64)
		if err != nil {
			return self.errE(err)
		}
		self.Lexeme.Real = f
		self.Token = TkReal
		return TkReal
	} else {
		i, err := strconv.ParseInt(buf.String(), 10, 64)
		if err != nil {
			return self.errE(err)
		}
		self.Lexeme.Int = i
		self.Token = TkInt
		return TkInt
	}
}

func (self *Lexer) lexStr(c rune) int {
	buf := &bytes.Buffer{}

	quote := c
	self.Cursor++
	self.Lexeme.Text = ""

	for {
		c, sz := self.nextRune()

		if c == utf8.RuneError {
			if sz == 0 {
				return self.err("string literal is not closed by quote properly")
			} else {
				return self.errUtf8()
			}
		}

		if c == quote {
			self.Cursor += sz
			break
		}

		if c == '\\' {
			cc := self.nextRune2()
			switch cc {
			case 't':
				self.Cursor++
				buf.WriteRune('\t')
				break

			case 'n':
				self.Cursor++
				buf.WriteRune('\n')
				break

			case 'b':
				self.Cursor++
				buf.WriteRune('\b')
				break

			case 'v':
				self.Cursor++
				buf.WriteRune('\v')
				break

			case 'r':
				self.Cursor++
				buf.WriteRune('\r')
				break

			case '\'':
				self.Cursor++
				buf.WriteRune('\'')
				break

			case '"':
				self.Cursor++
				buf.WriteRune('"')
				break

			case '\\':
				self.Cursor++
				buf.WriteRune('\\')
				break

			default:
				return self.err("unknown escape sequences inside of string literal")
			}
		} else {
			buf.WriteRune(c)
		}

		self.Cursor += sz
	}

	self.Lexeme.Text = buf.String()
	self.Token = TkStr
	return self.Token
}

func (self *Lexer) matchkeyword(str string, offset int) bool {
	c := self.Cursor + offset
	tar := []rune(str)

	for idx := 0; idx < len(tar); idx++ {
		r, sz := utf8.DecodeRuneInString(self.Source[c:]) // make sure to be case insensitive

		if unicode.ToLower(r) != tar[idx] {
			return false
		}
		c += sz
	}

	r, _ := utf8.DecodeRuneInString(self.Source[c:])
	if self.isIdChar(r) {
		return false
	} else {
		return true
	}
}

func (self *Lexer) matchKeyword(w string) bool {
	return self.matchkeyword(w, 1)
}

func (self *Lexer) matchKeyword2(w1, w2 string) (bool, int) {
	if !self.matchKeyword(w1) {
		return false, -1
	}

	off := 1 + len(w1)

	// skip all the whitespace that is in between
	for {
		r, _ := utf8.DecodeRuneInString(self.Source[self.Cursor+off:])
		if self.isWS(r) {
			off++
		} else {
			break
		}
	}

	if self.Cursor+off >= len(self.Source) {
		return false, -1
	}

	if self.matchkeyword(w2, off) {
		return true, off + len(w2)
	} else {
		return false, -1
	}
}

func (self *Lexer) isWS(r rune) bool {
	switch r {
	case ' ', '\r', '\t', '\n', '\b', '\v':
		return true
	default:
		return false
	}
}

func (self *Lexer) isIdChar(r rune) bool {
	// FIXME(dpeng):
	// This is not correct, what we want is ASCII's definition of number letter :(
	return r == '_' || unicode.IsLetter(r) || unicode.IsNumber(r)
}

func (self *Lexer) isIdLeadingChar(r rune) bool {
	// FIXME(dpeng):
	// This is not correct, what we want is ASCII's definition of number letter :(
	return r == '_' || r == '$' || unicode.IsLetter(r)
}

func (self *Lexer) tryKeyword(c rune) (bool, int) {
	switch c {
	case 'a', 'A':
		if self.matchKeyword("nd") {
			return true, self.yield(TkAnd, 3)
		}
		if self.matchKeyword("s") {
			return true, self.yield(TkAs, 2)
		}
		break
	case 'b', 'B':
		if self.matchKeyword("etween") {
			return true, self.yield(TkBetween, 7)
		}
		break

	case 'c', 'C':
		if self.matchKeyword("ast") {
			return true, self.yield(TkCast, 4)
		}
		if self.matchKeyword("ase") {
			return true, self.yield(TkCase, 4)
		}
		break

	case 'd', 'D':
		if self.matchKeyword("istinct") {
			return true, self.yield(TkDistinct, 8)
		}
		if self.matchKeyword("efault") {
			return true, self.yield(TkDefault, 7)
		}
		break

	case 'e', 'E':
		if self.matchKeyword("nd") {
			return true, self.yield(TkEnd, 3)
		}
		if self.matchKeyword("lse") {
			return true, self.yield(TkElse, 4)
		}
		break

	case 'f', 'F':
		if self.matchKeyword("alse") {
			return true, self.yield(TkFalse, 5)
		}
		if self.matchKeyword("rom") {
			return true, self.yield(TkFrom, 4)
		}
		if self.matchKeyword("ormat") {
			return true, self.yield(TkFormat, 6)
		}
		break

	case 'g', 'G':
		if yes, length := self.matchKeyword2("roup", "by"); yes {
			return true, self.yield(TkGroupBy, length)
		}
		break

	case 'h', 'H':
		if self.matchKeyword("aving") {
			return true, self.yield(TkHaving, 6)
		}
		break

	case 'i', 'I':
		if self.matchKeyword("n") {
			return true, self.yield(TkIn, 2)
		}
		if self.matchKeyword("f") {
			return true, self.yield(TkIf, 2)
		}
		break

	case 'l', 'L':
		if self.matchKeyword("imit") {
			return true, self.yield(TkLimit, 6)
		}
		break

	case 'n', 'N':
		if self.matchKeyword("ull") {
			return true, self.yield(TkNull, 4)
		}
		if self.matchKeyword("il") {
			return true, self.yield(TkNull, 3)
		}

		// always put at very last
		if self.matchKeyword("ot") {
			return true, self.yield(TkNot, 3)
		}
		break

	case 'o', 'O':
		if self.matchKeyword("r") {
			return true, self.yield(TkOr, 2)
		}
		if self.matchKeyword("utput") {
			return true, self.yield(TkOutput, 6)
		}
		if yes, l := self.matchKeyword2("rder", "by"); yes {
			return true, self.yield(TkOrderBy, l)
		}
		break

	case 'r', 'R':
		if self.matchKeyword("ewrite") {
			return true, self.yield(TkRewrite, 6)
		}
		break

	case 's', 'S':
		if self.matchKeyword("elect") {
			return true, self.yield(TkSelect, 6)
		}
		break

	case 't', 'T':
		if self.matchKeyword("rue") {
			return true, self.yield(TkTrue, 4)
		}
		if self.matchKeyword("hen") {
			return true, self.yield(TkThen, 4)
		}
		break

	case 'w', 'W':
		if self.matchKeyword("here") {
			return true, self.yield(TkWhere, 5)
		}
		break

	default:
		break
	}

	return false, 0
}

func (self *Lexer) lexId(c rune) int {
	if !self.isIdLeadingChar(c) {
		return self.err("invalid leading character of identifier")
	}

	buf := &bytes.Buffer{}
	if c != '$' {
		buf.WriteRune(unicode.ToLower(c))
	} else {
		buf.WriteRune('$')
	}
	self.Cursor++

	for {
		c, sz := self.nextRune()
		if c == utf8.RuneError {
			break
		}
		if !self.isIdChar(c) {
			break
		}
		self.Cursor += sz
		buf.WriteRune(unicode.ToLower(c))
	}

	self.Lexeme.Text = buf.String()
	self.Token = TkId
	return TkId
}

func (self *Lexer) lexKeywordOrId(c rune) int {
	yes, tk := self.tryKeyword(c)
	if yes {
		return tk
	}

	return self.lexId(c)
}

func (self *Lexer) Next() int {
	if self.Token == TkEof {
		return TkEof
	}

	if self.Cursor == len(self.Source) {
		self.Token = TkEof
		return TkEof
	}

	return self.next()
}

func (self *Lexer) next() int {
	for {
		c, sz := self.nextRune()
		if c == utf8.RuneError {
			if sz == 0 {
				return self.eof()
			} else {
				return self.errUtf8()
			}
		}

		switch c {
		case ',':
			return self.yield(TkComma, 1)

		case ':':
			if self.nextRune2() == ':' {
				return self.yield(TkDColon, 2)
			} else {
				return self.yield(TkColon, 1)
			}

		case ';':
			return self.yield(TkSemicolon, 1)

		case '.':
			return self.yield(TkDot, 1)

		case '?':
			return self.yield(TkQuestion, 1)

		case '[':
			return self.yield(TkLSqr, 1)

		case ']':
			return self.yield(TkRSqr, 1)

		case '{':
			return self.yield(TkLBra, 1)
		case '}':
			return self.yield(TkRBra, 1)

		case '(':
			return self.yield(TkLPar, 1)
		case ')':
			return self.yield(TkRPar, 1)

		case '+':
			return self.yield(TkAdd, 1)
		case '-':
			return self.yield(TkSub, 1)
		case '*':
			return self.yield(TkMul, 1)
		case '/':
			cc := self.nextRune2()
			if cc == '/' {
				self.Cursor += 2
				if !self.lexLineComment() {
					return self.Token
				} else {
					break
				}
			} else if cc == '*' {
				self.Cursor += 2
				if !self.lexBlockComment() {
					return self.Token
				} else {
					break
				}
			} else {
				return self.yield(TkDiv, 1)
			}

		case '%':
			return self.yield(TkMod, 1)

		case '&':
			if self.nextRune2() == '&' {
				return self.yield(TkAnd, 2)
			}
			return self.err("are you missing '&' for and operator?")

		case '|':
			if self.nextRune2() == '|' {
				return self.yield(TkOr, 2)
			}
			return self.err("are you missing '|' for or operator?")

		case '=':
			if self.nextRune2() == '=' {
				return self.yield(TkEq, 2)
			} else {
				return self.yield(TkAssign, 1)
			}

		case '>':
			if self.nextRune2() == '=' {
				return self.yield(TkGe, 2)
			} else {
				return self.yield(TkGt, 1)
			}

		case '<':
			if self.nextRune2() == '=' {
				return self.yield(TkLe, 2)
			} else if self.nextRune2() == '>' {
				return self.yield(TkEq, 2)
			} else {
				return self.yield(TkLt, 1)
			}

		case '!':
			if self.nextRune2() == '=' {
				return self.yield(TkNe, 2)
			} else {
				return self.yield(TkNot, 1)
			}

		case ' ', '\r', '\t', '\n', '\b', '\v':
			self.Cursor++
			break

		case '\'', '"':
			return self.lexStr(c)

		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			return self.lexNum(c)

		case '#':
			if !self.lexLineComment() {
				self.Cursor++
				return self.Token
			}
			break

		default:
			return self.lexKeywordOrId(c)
		}
	}
}

func (self *Lexer) lowerText() string {
	return strings.ToLower(self.Lexeme.Text)
}

func newLexer(source string) *Lexer {
	return &Lexer{
		Source: source,
		Cursor: 0,
		Token:  TkError,
	}
}
