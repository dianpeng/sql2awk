package cg

import (
	"fmt"
	"github.com/dianpeng/sql2awk/plan"
	"github.com/dianpeng/sql2awk/sql"
	"strings"
)

const (
	AwkGnuAwk = iota
	AwkGoAwk
	AwkAwk
	AwkNAwk
	AwkMAwk
	AwkFrawk // rust performant implementation
)

type Config struct {
	OutputSeparator string
	AwkType         int
}

const outputAlign = 16

func Generate(x *plan.Plan, config *Config) (string, error) {
	g := &queryCodeGen{
		OutputSeparator: config.OutputSeparator,
		query:           x,
		awkType:         config.AwkType,
	}
	return g.Gen()
}

// codegen from plan to *awk* code. Notes, this pass does not generate sort
// instruction.

type queryCodeGen struct {
	OutputSeparator string
	query           *plan.Plan
	g               awkGlobal
	tsRef           []tableScanGenRef
	awkType         int
}

type subGen interface {
	genNext() error
	genFlush() error
	genDone() error
	setWriter(*awkWriter)
}

func (self *queryCodeGen) formatSep() string {
	return self.query.Format.GetBorderString()
}

func (self *queryCodeGen) formatPaddingSize() int {
	return self.query.Format.Padding.IntOption
}

func (self *queryCodeGen) outputSize() int {
	return len(self.query.Output.VarList)
}

func (self *queryCodeGen) tsSize() int {
	return len(self.query.TableScan)
}

func (self *queryCodeGen) varTable(x int) string {
	return fmt.Sprintf("tbl_%d", x)
}

func (self *queryCodeGen) varTableSize(x int) string {
	return fmt.Sprintf("tblsize_%d", x)
}

func (self *queryCodeGen) varTableField(x int) string {
	return fmt.Sprintf("tblfnum_%d", x)
}

func (self *queryCodeGen) varRID(x int) string {
	return fmt.Sprintf("rid_%d", x)
}

func (self *queryCodeGen) varAggTable() string {
	return "agg"
}

func (self *queryCodeGen) genGlobal() string {
	ts := self.tsSize()
	lines := []string{}

	for i := 0; i < ts; i++ {
		lines = append(lines, fmt.Sprintf("  %s[\"\"] = 0", self.varTable(i)))
		lines = append(lines, fmt.Sprintf("  %s = 0", self.varTableSize(i)))
		lines = append(lines, fmt.Sprintf("  %s = 0", self.varTableField(i)))
	}
	lines = append(lines, fmt.Sprintf("  agg[\"\"] = 0"))

	return strings.Join(lines, "\n")
}

func (self *queryCodeGen) genExprAsStr(
	e sql.Expr,
) string {
	gen := &exprCodeGen{
		cg: self,
	}
	gen.genExprAsStr(e)
	return gen.o.String()
}

func (self *queryCodeGen) genExpr(
	e sql.Expr,
) string {
	gen := &exprCodeGen{
		cg: self,
	}
	gen.genExpr(e)
	return gen.o.String()
}

func (self *queryCodeGen) genTableScan() (string, error) {
	writer, g := newAwkWriter(
		0,
		"",
	)
	self.g.addG(g)
	gen := &tableScanGen{
		cg:     self,
		writer: writer,
	}
	if err := gen.gen(self.query); err != nil {
		return "", err
	}
	self.tsRef = gen.Ref
	return writer.Flush(), nil
}

func (self *queryCodeGen) genJoin() string {
	writer, g := newAwkWriter(
		0,
		"join",
	)
	self.g.addG(g)
	gen := joinCodeGen{
		cg: self,
	}
	gen.genJoin(writer)
	return writer.Flush()
}

func (self *queryCodeGen) genNext(
	gen subGen,
	name string,
	n int,
) (string, error) {
	writer, g := newAwkWriter(
		n,
		name,
	)
	self.g.addG(g)
	gen.setWriter(writer)
	if err := gen.genNext(); err != nil {
		return "", err
	}
	return writer.Flush(), nil
}

func (self *queryCodeGen) genFlush(
	gen subGen,
	name string,
	n int,
) (string, error) {
	writer, g := newAwkWriter(
		n,
		name,
	)
	self.g.addG(g)
	gen.setWriter(writer)
	if err := gen.genFlush(); err != nil {
		return "", err
	}
	return writer.Flush(), nil
}

func (self *queryCodeGen) genDone(
	gen subGen,
	name string,
	n int,
) (string, error) {
	writer, g := newAwkWriter(
		n,
		name,
	)
	self.g.addG(g)
	gen.setWriter(writer)
	if err := gen.genDone(); err != nil {
		return "", err
	}
	return writer.Flush(), nil
}

func (self *queryCodeGen) genSubGen(
	gen subGen,
	stage string,
) (string, error) {
	buf := strings.Builder{}
	if data, err := self.genNext(
		gen,
		fmt.Sprintf("%s_next", stage),
		self.tsSize(),
	); err != nil {
		return "", err
	} else {
		buf.WriteString(data)
	}

	if data, err := self.genFlush(
		gen,
		fmt.Sprintf("%s_flush", stage),
		0,
	); err != nil {
		return "", err
	} else {
		buf.WriteString(data)
	}

	if data, err := self.genDone(
		gen,
		fmt.Sprintf("%s_done", stage),
		0,
	); err != nil {
		return "", err
	} else {
		buf.WriteString(data)
	}

	return buf.String(), nil
}

func (self *queryCodeGen) genSubGenN(
	gen subGen,
	stage string,
	n1 int,
	n2 int,
	n3 int,
) (string, error) {
	buf := strings.Builder{}
	if data, err := self.genNext(
		gen,
		fmt.Sprintf("%s_next", stage),
		n1,
	); err != nil {
		return "", err
	} else {
		buf.WriteString(data)
	}

	if data, err := self.genFlush(
		gen,
		fmt.Sprintf("%s_flush", stage),
		n2,
	); err != nil {
		return "", err
	} else {
		buf.WriteString(data)
	}

	if data, err := self.genDone(
		gen,
		fmt.Sprintf("%s_done", stage),
		n3,
	); err != nil {
		return "", err
	} else {
		buf.WriteString(data)
	}

	return buf.String(), nil
}

func (self *queryCodeGen) genGroupBy() (string, error) {
	gen := &groupByCodeGen{
		cg:     self,
		tsSize: self.tsSize(),
	}
	return self.genSubGen(gen, "group_by")
}

func (self *queryCodeGen) genAgg() (string, error) {
	gen := &aggCodeGen{
		cg: self,
	}
	return self.genSubGen(gen, "agg")
}

func (self *queryCodeGen) genHaving() (string, error) {
	gen := &havingCodeGen{
		cg: self,
	}
	return self.genSubGen(gen, "having")
}

func (self *queryCodeGen) genSort() (string, error) {
	gen := &sortCodeGen{
		cg: self,
	}
	return self.genSubGen(gen, "sort")
}

func (self *queryCodeGen) genOutput() (string, error) {
	gen := newOutputCodeGen(self)
	return self.genSubGen(gen, "output")
}

func (self *queryCodeGen) genFormat() (string, error) {
	gen := &formatCodeGen{
		cg: self,
	}
	return self.genSubGenN(gen, "format", len(self.query.Output.VarList), 0, 0)
}

func (self *queryCodeGen) genBegin() string {
	buf := &strings.Builder{}
	for _, g := range self.g.globalList() {
		if !g.array {
			buf.WriteString(fmt.Sprintf("  %s\n", g.name))
		}
	}
	buf.WriteString(self.genGlobal())
	return buf.String()
}

func (self *queryCodeGen) genFormatBuiltin() string {
	buf := &strings.Builder{}

	{
		c, f := generateFormatFallbackFormat(self)
		self.g.addG(f)
		buf.WriteString(c)
	}

	{
		c, f := generateFormatWildcardFallbackPrint(self)
		self.g.addG(f)
		buf.WriteString(c)
	}

	{
		c, f := generateFormatWildcardPrintColumn(self)
		self.g.addG(f)
		buf.WriteString(c)
	}

	{
		c, f := generateFormatPrologue(self)
		self.g.addG(f)
		buf.WriteString(c)
	}

	{
		c, f := generateFormatEpilogue(self)
		self.g.addG(f)
		buf.WriteString(c)
	}

	{
		c, f := generateWildcardTitleFoot(self)
		self.g.addG(f)
		buf.WriteString(c)
	}

	{
		c, f := generateWildcardTitle(self)
		self.g.addG(f)
		buf.WriteString(c)
	}

	return buf.String()
}

func (self *queryCodeGen) Gen() (string, error) {
	tableScan := ""
	join := ""
	groupBy := ""
	agg := ""
	having := ""
	sort := ""
	output := ""
	format := ""

	if ts, err := self.genTableScan(); err != nil {
		return "", err
	} else {
		tableScan = ts
	}

	join = self.genJoin()

	if x, err := self.genGroupBy(); err != nil {
		return "", err
	} else {
		groupBy = x
	}

	if x, err := self.genAgg(); err != nil {
		return "", err
	} else {
		agg = x
	}

	if x, err := self.genHaving(); err != nil {
		return "", err
	} else {
		having = x
	}

	if x, err := self.genSort(); err != nil {
		return "", err
	} else {
		sort = x
	}

	if x, err := self.genOutput(); err != nil {
		return "", err
	} else {
		output = x
	}

	if x, err := self.genFormat(); err != nil {
		return "", err
	} else {
		format = x
	}
	formatBuiltin := self.genFormatBuiltin()

	builtinMisc := ""
	switch self.awkType {
	case AwkGoAwk:
		builtinMisc = builtinGoAWK
		break
	default:
		break
	}

	// finally our skeletong will be done here
	return fmt.Sprintf(
		`
# -----------------------------------------------------------------
# Globals
# -----------------------------------------------------------------
BEGIN {
%s
}

# -----------------------------------------------------------------
# Table Scan
# -----------------------------------------------------------------
{
%s
}

END {
  format_prologue();
  join();
  format_epilogue();
}

# -----------------------------------------------------------------
# join
# -----------------------------------------------------------------
%s

# -----------------------------------------------------------------
# group by
# -----------------------------------------------------------------
%s

# -----------------------------------------------------------------
# agg
# -----------------------------------------------------------------
%s

# -----------------------------------------------------------------
# having
# -----------------------------------------------------------------
%s

# -----------------------------------------------------------------
# sort
# -----------------------------------------------------------------
%s

# -----------------------------------------------------------------
# output
# -----------------------------------------------------------------
%s

# -----------------------------------------------------------------
# format
# -----------------------------------------------------------------
%s
%s

# -----------------------------------------------------------------
# builtins
# -----------------------------------------------------------------
%s
%s
`,
		self.genBegin(), // always *LAST*, need to collect globals
		tableScan,
		join,
		groupBy,
		agg,
		having,
		sort,
		output,
		format,
		formatBuiltin,
		builtinAWK,
		builtinMisc,
	), nil
}

/**

BEGIN {
}

{
  // table scan ------------------------------------------------
}

END {
  join()
}

function join() {
  ...
}

function group_by_next(...) {
}

function group_by_flush() {
}

function group_by_done() {
}

function agg_next(...) {
}

function agg_flush() {
}

function agg_done() {
}

function having_next(...) {
}

function having_flush() {
}

function having_done() {
}

function sort_next(...) {
}

function sort_flush() {
}

function sort_done() {
}

function output_next(...) {
}

function output_flush() {
}

function output_done() {
}

**/
