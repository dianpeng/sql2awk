package cg

import (
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
)

type awkWriterCtx map[string]interface{}

type awkGlobalFromFunc struct {
	funcName string
	g        []string
}

// collector used to flush awkWriter's global definition into. Used for us
// to clearly declare the global in the BEGIN scope of awk. This better
// for visualization of code generation and debug purpose.
type awkGlobal struct {
	G []*awkGlobalFromFunc
}

// A special template engine used for *AWK* source code dump. We call it awk
// writer, but in fact it can do anything, TBH.

type awkWriter struct {
	g      *awkGlobalFromFunc // global from func collector
	indent int                // current indent level for formatting
	buf    strings.Builder    // output buffer, notes, the function protocol is gen
	// when the body is done, since we need to collect
	// local count and local name
	local       []string        // locals
	localIndex  map[string]bool // used to dedup
	global      []string        // globals
	globalIndex map[string]bool // used to dedup

	param int // parameter #, same as *len of table scan list*. Notes all the
	// parameter for this function will be prefixed with fixed one
	funcName string // notes, AWK does not support nested function, or first order

	// function, we do not need to maintain multiple funcName,
	// if this field is "", we are in global scope
}

func (self *awkGlobal) addG(g *awkGlobalFromFunc) {
	self.G = append(self.G, g)
}

func (self *awkGlobal) globalList() []string {
	out := []string{}
	idx := make(map[string]bool)
	for _, x := range self.G {
		for _, y := range x.g {
			if idx[y] {
				continue
			}
			idx[y] = true
			out = append(out, y)
		}
	}
	return out
}

func newAwkWriter(
	parSize int,
	funcName string,
) (*awkWriter, *awkGlobalFromFunc) {
	g := &awkGlobalFromFunc{
		funcName: funcName,
	}
	writer := &awkWriter{
		g:           g,
		indent:      1,
		localIndex:  make(map[string]bool),
		globalIndex: make(map[string]bool),
		param:       parSize,
		funcName:    funcName,
	}
	return writer, g
}

func (self *awkWriter) Flush() string {
	out := strings.Builder{}

	body := self.buf.String() // body
	paramList := []string{}
	localList := []string{}

	for i := 0; i < self.param; i++ {
		paramList = append(paramList, fmt.Sprintf("rid_%d", i))
	}
	localList = self.local

	parStr := strings.Join(paramList, ", ")
	locStr := strings.Join(localList, ", ")

	// AWK function's parameter list cannot be defined cross multiple line. To
	// use normal AWK, your function variable has to be in the same line as
	// everything

	out.WriteString(fmt.Sprintf("function %s(", self.funcName))
	out.WriteString(parStr)

	if locStr != "" {
		if parStr != "" {
			out.WriteString(", ")
		}
		out.WriteString(locStr)
	}

	out.WriteString(") {\n")
	out.WriteString(body)
	out.WriteString("}\n")

	return out.String()
}

func (self *awkWriter) HasLocal(l string) bool {
	n := self.localVarName(l, -1)
	_, ok := self.localIndex[n]
	return ok
}

func (self *awkWriter) HasLocalN(l string, idx int) bool {
	n := self.localVarName(l, idx)
	_, ok := self.localIndex[n]
	return ok
}

func (self *awkWriter) DefineLocal(
	n string,
) {
	self.Local(n)
}

func (self *awkWriter) DefineLocalN(
	n string,
	idx int,
) {
	self.LocalN(n, idx)
}

func (self *awkWriter) Local(
	n string,
) string {
	if !self.HasLocal(n) {
		n = self.localVarName(n, -1)
		self.localIndex[n] = true
		self.local = append(self.local, n)
	}
	return n
}

func (self *awkWriter) LocalN(
	prefix string,
	idx int,
) string {
	n := self.localVarName(prefix, idx)
	if !self.HasLocalN(prefix, idx) {
		n = self.localVarName(prefix, idx)
		self.localIndex[n] = true
		self.local = append(self.local, n)
	}
	return n
}

func (self *awkWriter) localVarName(
	n string,
	idx int,
) string {
	if idx < 0 {
		return fmt.Sprintf("local_%s", n)
	} else {
		return fmt.Sprintf("local_%s_%d", n, idx)
	}
}

func (self *awkWriter) HasGlobal(l string) bool {
	g := self.globalVarName(l, -1)
	_, ok := self.globalIndex[g]
	return ok
}

func (self *awkWriter) HasGlobalN(l string, idx int) bool {
	g := self.globalVarName(l, idx)
	_, ok := self.globalIndex[g]
	return ok
}

func (self *awkWriter) DefineGlobal(
	n string,
) {
	self.Global(n)
}

func (self *awkWriter) globalVarName(
	n string,
	idx int,
) string {
	if idx < 0 {
		return fmt.Sprintf("global_%s", n)
	} else {
		return fmt.Sprintf("global_%s_%d", n, idx)
	}
}

func (self *awkWriter) Global(
	n string,
) string {
	n = self.globalVarName(n, -1)
	if !self.HasGlobal(n) {
		self.globalIndex[n] = true
		self.global = append(self.global, n)
		self.g.g = append(self.g.g, n)
	}
	return n
}

func (self *awkWriter) GlobalN(
	n string,
	idx int,
) string {
	return self.Global(fmt.Sprintf("%s_%d", n, idx))
}

func (self *awkWriter) RID(i int) string {
	if i >= self.param {
		panic("invalid RID")
	}
	return fmt.Sprintf("rid_%d", i)
}

// ----------------------------------------------------------------------------
//
// Substitution phase, ie the meta instruction inside of the template should
// be handled properly to allow certain things. Basically we allow 3 types of
// special substitution.
//
// 1. %[var0, var1, ..., %(lit), ..., varN] general substituion
// 2. @[instruction, ...]
// 3. #[special_known_name, ...]
// 4. $[var, ...]
//
// Instruction is kind of special, it allows user to call certain special
// instruction of the writer, for example, to call pipeline function Next
//
// @[pipeline_next, %(group_by)]
//
// ----------------------------------------------------------------------------

type awkWriterCmd struct {
	cmd    string   // command's name
	cmdSub string   // substituion of first one, may be command name, can be nil
	arg    []string // command's argument, after substituion
}

const (
	asciiPercent = 0x25
	asciiLPar    = 0x28
	asciiRPar    = 0x29
)

func parseAwkWriterCmd(
	input string,
	ctx awkWriterCtx,
	sub bool,
) (*awkWriterCmd, error) {
	x := strings.Split(input, ",")
	if len(x) == 0 {
		return nil, fmt.Errorf("invalid argument, at least one should be specified")
	}
	for idx, v := range x {
		x[idx] = strings.TrimSpace(v)
	}

	cmd := x[0]
	cmdSub := ""
	arg := x[1:]

	if sub {
		if vvv, ok := ctx[cmd]; ok {
			cmdSub = fmt.Sprintf("%v", vvv)
		}
		for idx, v := range arg {
			byteV := []byte(v)
			if len(byteV) > 1 &&
				byteV[0] == asciiPercent &&
				byteV[1] == asciiLPar &&
				byteV[len(v)-1] == asciiRPar {
				vv := byteV[2 : len(v)-1]
				arg[idx] = string(vv)
			} else {
				// do the substitution
				if vvv, ok := ctx[v]; ok {
					arg[idx] = fmt.Sprintf("%v", vvv)
				} else {
					return nil, fmt.Errorf("variable(%s) is not found", v)
				}
			}
		}
	}

	return &awkWriterCmd{
		cmd:    x[0],
		cmdSub: cmdSub,
		arg:    arg,
	}, nil
}

func (self *awkWriter) processSubCmd(
	cmd *awkWriterCmd,
) (string, error) {
	if len(cmd.arg) == 0 {
		return cmd.cmdSub, nil
	} else {
		tt := []string{
			cmd.cmdSub,
		}
		tt = append(tt, cmd.arg...)
		return strings.Join(tt, "_"), nil
	}
}

func (self *awkWriter) pipelineCallParams() []string {
	x := []string{}
	for i := 0; i < self.param; i++ {
		x = append(x, fmt.Sprintf("rid_%d", i))
	}
	return x
}

func (self *awkWriter) processInsPipeline(
	target string,
	ty string,
) (string, error) {
	name := fmt.Sprintf("%s_%s", target, ty)
	if ty == "next" {
		return name + "(" + strings.Join(self.pipelineCallParams(), ", ") + ")", nil
	} else {
		return fmt.Sprintf("%s()", name), nil
	}
}

func (self *awkWriter) processInsCmd(
	cmd *awkWriterCmd,
) (string, error) {
	switch cmd.cmd {
	case "pipline_next", "pipeline_flush", "pipeline_done":
		if len(cmd.arg) != 1 {
			return "", fmt.Errorf("pipeline_next: invalid argument")
		}
		x := ""
		switch cmd.cmd {
		case "pipeline_next":
			x = "next"
			break

		case "pipeline_flush":
			x = "flush"
			break

		default:
			x = "done"
			break
		}
		return self.processInsPipeline(cmd.arg[0], x)
	default:
		return "", fmt.Errorf("unknown instruction %s", cmd.cmd)
	}
}

func (self *awkWriter) processSVarCmd(
	cmd *awkWriterCmd,
) (string, error) {
	switch cmd.cmd {
	case "rid": // known to us
		if len(cmd.arg) != 1 {
			return "", fmt.Errorf("invalid argument for rid")
		}
		if _, err := strconv.Atoi(cmd.arg[0]); err != nil {
			return "", fmt.Errorf("invalid 1st argument, must be index/integer")
		}
		return fmt.Sprintf("rid_%s", cmd.arg[0]), nil

	default:
		return "", nil
	}
}

func (self *awkWriter) processVarCmd(
	cmd *awkWriterCmd,
) (string, error) {
	switch strings.ToLower(cmd.cmd) {
	case "g", "global":
		if len(cmd.arg) != 1 {
			return "", fmt.Errorf("global variable name has too many components")
		}
		self.DefineGlobal(cmd.arg[0])
		return self.globalVarName(cmd.arg[0], -1), nil
	case "l", "local":
		if len(cmd.arg) == 1 {
			self.DefineLocal(cmd.arg[0])
			return self.localVarName(cmd.arg[0], -1), nil
		} else if x, err := strconv.Atoi(cmd.arg[1]); err == nil && x >= 0 {
			self.DefineLocalN(cmd.arg[0], x)
			return self.localVarName(cmd.arg[0], x), nil
		} else {
			return "", fmt.Errorf("global variable name has too many components")
		}
	default:
		return "", fmt.Errorf("unknown variable type in $ expression")
	}
}

func (self *awkWriter) subLine(
	l string,
	ctx awkWriterCtx,
) string {
	if ctx == nil {
		ctx = make(awkWriterCtx)
	}

	out := strings.Builder{}
	charAt := func(lit string, x int) rune {
		c, _ := utf8.DecodeRuneInString(lit[x:])
		return c
	}
	cursor := 0

	for {
		pos := strings.IndexAny(
			l[cursor:],
			"%@#$",
		)
		if pos == -1 {
			out.WriteString(l[cursor:])
			break
		}
		if charAt(l, cursor+pos+1) != '[' {
			// okay, not an expected sequence
			out.WriteString(l[cursor : cursor+pos+1])
			cursor = (cursor + pos + 1)
			continue
		}

		out.WriteString(l[cursor : cursor+pos])
		c, _ := utf8.DecodeRuneInString(l[cursor+pos:])
		doSub := true

		pos++ // skip leading
		endPos := strings.Index(l[cursor+pos:], "]")
		if endPos == -1 {
			panic("expect ']' to finish sub parameter's argument list")
		}
		if c == '$' {
			doSub = false // variable do not perform sub
		}
		nameOrParams := l[cursor+pos+1 : cursor+pos+endPos]
		cmd, err := parseAwkWriterCmd(nameOrParams, ctx, doSub)

		if err != nil {
			panic(err.Error())
		}

		switch c {
		case '%':
			if data, err := self.processSubCmd(cmd); err != nil {
				panic(err.Error())
			} else {
				out.WriteString(data)
			}
			break
		case '@':
			if data, err := self.processInsCmd(cmd); err != nil {
				panic(err.Error())
			} else {
				out.WriteString(data)
			}
			break
		case '#':
			if data, err := self.processSVarCmd(cmd); err != nil {
				panic(err.Error())
			} else {
				out.WriteString(data)
			}
			break
		case '$':
			if data, err := self.processVarCmd(cmd); err != nil {
				panic(err.Error())
			} else {
				out.WriteString(data)
			}
			break
		default:
			break
		}

		cursor += (pos + endPos + 1)
	}
	return out.String()
}

func (self *awkWriter) o(x string) {
	self.buf.WriteString(x)
}

func (self *awkWriter) oLB() {
	self.o("\n")
}

func (self *awkWriter) oIndent() {
	self.buf.WriteString(strings.Repeat("  ", self.indent))
}

func (self *awkWriter) oLine(line string, ctx awkWriterCtx) {
	self.oIndent()
	self.o(self.subLine(line, ctx))
	self.oLB()
}

func (self *awkWriter) oChunk(lines string, ctx awkWriterCtx) {
	lineArray := strings.Split(lines, "\n")
	for _, x := range lineArray {
		self.oLine(x, ctx)
	}
}

func (self *awkWriter) Line(
	line string,
	ctx awkWriterCtx,
) {
	self.oLine(line, ctx)
}

func (self *awkWriter) Chunk(
	chunk string,
	ctx awkWriterCtx,
) {
	self.oChunk(chunk, ctx)
}

func (self *awkWriter) CallAny(
	name string,
	args []interface{},
) {
	self.Call(name, self.toStringList(args))
}

func (self *awkWriter) Call(
	name string,
	args []string,
) {
	self.oIndent()
	self.o(name)
	self.o("(")
	self.o(strings.Join(args, ", "))
	self.o(");")
	self.oLB()
}

func (self *awkWriter) ParamList(
	prefix string,
	count int,
) []string {
	out := []string{}
	for i := 0; i < count; i++ {
		out = append(out, fmt.Sprintf("%s_%d", prefix, i))
	}
	return out
}

func (self *awkWriter) GlobalParamList(
	prefix string,
	count int,
) []string {
	out := []string{}
	for i := 0; i < count; i++ {
		out = append(out, self.globalVarName(prefix, i))
	}
	return out
}

func (self *awkWriter) CallPipelineNext(
	name string,
) {
	self.Call(
		fmt.Sprintf("%s_next", name),
		self.pipelineCallParams(),
	)
}

func (self *awkWriter) CallPipelineFlush(
	name string,
) {
	self.Call(
		fmt.Sprintf("%s_flush", name),
		nil,
	)
}

func (self *awkWriter) CallPipelineDone(
	name string,
) {
	self.Call(
		fmt.Sprintf("%s_done", name),
		nil,
	)
}

func (self *awkWriter) Assign(
	varName string,
	valExpr string,
	ctx awkWriterCtx,
) {
	self.Line(
		fmt.Sprintf("%s = %s;", varName, valExpr),
		ctx,
	)
}

func (self *awkWriter) ArrIdxN(
	v string,
	i int,
) string {
	return fmt.Sprintf("%s[%d]", v, i)
}

func (self *awkWriter) ArrIdx(
	v string,
	i string,
) string {
	return fmt.Sprintf("%s[%s]", v, i)
}

func (self *awkWriter) Fmt(
	f string,
	ctx awkWriterCtx,
) string {
	return self.subLine(f, ctx)
}

func (self *awkWriter) For(
	header string,
	ctx awkWriterCtx,
) {
	self.Line(fmt.Sprintf("for (%s) {", header), ctx)
	self.indent++
}

func (self *awkWriter) ForEnd() {
	self.indent--
	self.Line("}", nil)
}

func (self *awkWriter) toStringList(x []interface{}) []string {
	out := []string{}
	for _, xx := range x {
		out = append(out, fmt.Sprintf("%s", xx))
	}
	return out
}
