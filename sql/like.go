package sql

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// ----------------------------------------------------------------------------
//
// SQL Like operator. Internally, the sql's LIKE operator will be modified and
// translated into regex
//
// The sql like's wildcard is relatively simple, basically supports 2 placeholder
//
// 1. %, represents zero, one or more sequnces of any characters
// 2. _, represents exactly one character
// 3. the escape is by %[x] syntax, which is kind of messy, TBH
//   3.1 %[] is not a syntax error, YOU ARE ALWAYS EXPECT ONE EXACT CHAR INSIDE
//
// currently we have 2 set of implementation, one in AWK and the other in GO,
// the go one will be used during parsing, which will just translate any LIKE
// with literal *RHS* to be a MATCH operator. This is to achieve better efficiency
//
// ----------------------------------------------------------------------------

func LikeToRegex(
	input string,
) string {
	buf := strings.Builder{}
	buf.WriteString("^")

	// scanning from left to right until we hit special character
	l := len(input)

	encodeC := func(c rune) {
		switch c {
		case '[':
			buf.WriteString("\\[")
			break

		case ']':
			buf.WriteString("\\]")
			break

		default:
			buf.WriteString(fmt.Sprintf("[%c]", c))
			break
		}
	}

	for i := 0; i < l; {
		c, xx := utf8.DecodeRuneInString(input[i:])
		if c == utf8.RuneError {
			i++
			continue // skip it
		}

		switch c {
		case '%':

			if i+3 <= l {
				ntk := input[i+1]
				if ntk == '[' {
					if inner, sz := utf8.DecodeRuneInString(input[i+1:]); inner != utf8.RuneError {
						if i += (2 + sz); input[i] == ']' {
							encodeC(inner)
							i++
							continue
						}
					}
				}
			}

			buf.WriteString(".*")
			break

		case '_':
			buf.WriteString(".")
			break

		default:
			encodeC(c)
			break
		}

		i += xx
	}

	buf.WriteString("$")
	return buf.String()
}
