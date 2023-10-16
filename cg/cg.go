package cg

import (
	"fmt"
	"github.com/dianpeng/sql2awk/plan"
	"github.com/dianpeng/sql2awk/sql"
	"strings"
)

func Generate(x *plan.Plan) (string, error) {
	g := &queryCodeGen{
		OutputSeparator: " ",
		query:           x,
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
}

type subGen interface {
	genNext() error
	genFlush() error
	genDone() error
	setWriter(*awkWriter)
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

func (self *queryCodeGen) genExpr(
	e sql.Expr,
) string {
	gen := &exprCodeGen{
		cg: self,
	}
	gen.genExpr(e)
	return gen.o.String()
}

func (self *queryCodeGen) genTableScan() string {
	buf := strings.Builder{}
	gen := &tableScanGen{
		cg: self,
	}
	buf.WriteString(gen.gen(self.query))
	self.tsRef = gen.Ref
	return buf.String()
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
) (string, error) {
	writer, g := newAwkWriter(
		self.tsSize(),
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
) (string, error) {
	writer, g := newAwkWriter(
		0,
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
) (string, error) {
	writer, g := newAwkWriter(
		0,
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
	); err != nil {
		return "", err
	} else {
		buf.WriteString(data)
	}

	if data, err := self.genFlush(
		gen,
		fmt.Sprintf("%s_flush", stage),
	); err != nil {
		return "", err
	} else {
		buf.WriteString(data)
	}

	if data, err := self.genDone(
		gen,
		fmt.Sprintf("%s_done", stage),
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

func (self *queryCodeGen) genOutput() (string, error) {
	gen := newOutputCodeGen(self)
	return self.genSubGen(gen, "output")
}

func (self *queryCodeGen) genBegin() string {
	buf := &strings.Builder{}
	for _, g := range self.g.globalList() {
		buf.WriteString(fmt.Sprintf("  %s = \"\"\n", g))
	}
	buf.WriteString(self.genGlobal())
	return buf.String()
}

func (self *queryCodeGen) Gen() (string, error) {
	tableScan := ""
	join := ""
	groupBy := ""
	agg := ""
	having := ""
	output := ""

	tableScan = self.genTableScan()
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

	if x, err := self.genOutput(); err != nil {
		return "", err
	} else {
		output = x
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
  join();
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
# output
# -----------------------------------------------------------------
%s
`,
		self.genBegin(),
		tableScan,
		join,
		groupBy,
		agg,
		having,
		output,
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

function output_next(...) {
}

function output_flush() {
}

function output_done() {
}

**/
