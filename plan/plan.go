package plan

import (
	"fmt"
	"github.com/dianpeng/sql2awk/sql"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	AggMin = iota
	AggMax
	AggAvg
	AggSum
	AggCount
	AggPercentile
	AggHistogram
)

const (
	defMaxColumnSize = 600
	defMaxTableSize  = 100
)

func aggTypeToName(i int) string {
	switch i {
	case AggMin:
		return "min"
	case AggMax:
		return "max"
	case AggAvg:
		return "avg"
	case AggSum:
		return "sum"
	case AggCount:
		return "count"
	case AggPercentile:
		return "percentile"
	case AggHistogram:
		return "histogram"
	default:
		return "unknown"
	}
}

const (
	aggTableIndex       = -1
	wildcardTableIndex  = -2
	WildcardColumnIndex = math.MaxInt

	// Special column index, CodeGen will need to take care of it internally
	ColumnIndexNF     = math.MaxInt - 1
	ColumnIndexRowNum = math.MaxInt - 2
)

type Options []interface{}

// comming from Rewrite syntax. Used to represent manipulation of scanning of
// table.
type TableRewriteSet struct {
	Column int
	Value  sql.Expr
}

type TableRewriteStmt struct {
	Cond sql.Expr           // condition of expression
	Set  []*TableRewriteSet // set, len(Set) == 0 means ignore the current one
}

type TableRewrite struct {
	Stmt []*TableRewriteStmt
}

type TableDescriptor struct {
	Index      int
	Path       string
	Params     sql.ConstList
	Type       string
	Alias      string // table alias
	Options    Options
	Symbol     string       // table symbol name, used by code generation
	MaxColumn  int          // maximum column index know to us, at least one column
	Column     map[int]bool // list of column fields will be access
	FullColumn bool         // whehter require a full column dump here
	Rewrite    *TableRewrite
}

type TableScan struct {
	Table     *TableDescriptor // which table to be scanned
	RowFilter *TableMatcher    // must be a row filter or nil
	ColFilter *TableMatcher    // nust be a column filter or nil
	Filter    sql.Expr         // early filter, if none nil
}

type Join interface {
	JoinName() string
	JoinFilter() sql.Expr
	Dump() string
}

// The default one that will be used for now, in the future we will have better
// one since at least hash join can somehow be implemented inside of AWK
type NestedLoopJoin struct {
	Filter sql.Expr // nested join's filter
}

func (self *NestedLoopJoin) JoinName() string     { return "nested-loop" }
func (self *NestedLoopJoin) JoinFilter() sql.Expr { return self.Filter }

func (self *NestedLoopJoin) Dump() string {
	buf := strings.Builder{}
	buf.WriteString("##> Join\n")
	buf.WriteString("Name: nested-loop\n")
	buf.WriteString(fmt.Sprintf("Filter: %s\n", sql.PrintExpr(self.Filter)))
	return buf.String()
}

type GroupBy struct {
	VarList []sql.Expr // list of expression used for group by
}

// Aggregation phase. The way it works is that it exposes 2 interface,
// agg_next/agg_flush. After agg_flush is been emited, it will call next
// phase iterator
type AggVar struct {
	AggType int      // agg type
	Value   sql.Expr // expression of *AGG*
	Target  sql.Expr // target of AGG operation
}

func (self *AggVar) AggName() string { return aggTypeToName(self.AggType) }

func (self *AggVar) param(idx int) *sql.Const {
	suffix := self.Value.(*sql.Suffix)
	if idx >= len(suffix.Call.Parameters) {
		return nil
	}
	c := suffix.Call.Parameters[idx]
	if c.Type() != sql.ExprConst {
		return nil
	}
	return c.(*sql.Const)
}

func (self *AggVar) ParamInt(idx int) (int64, bool) {
	if c := self.param(idx); c != nil && c.Ty == sql.ConstInt {
		return c.Int, true
	}
	return 0, false
}

func (self *AggVar) ParamReal(idx int) (float64, bool) {
	if c := self.param(idx); c != nil && c.Ty == sql.ConstReal {
		return c.Real, true
	}
	return 0.0, false
}

func (self *AggVar) ParamNum(idx int) (string, bool) {
	if c := self.param(idx); c != nil {
		switch c.Ty {
		case sql.ConstReal:
			return fmt.Sprintf("%f", c.Real), true
		case sql.ConstInt:
			return fmt.Sprintf("%d", c.Int), true
		default:
			break
		}
	}
	return "", false
}

func (self *AggVar) ParamStr(idx int) (string, bool) {
	if c := self.param(idx); c != nil && c.Ty == sql.ConstStr {
		return c.String, true
	}
	return "", false
}

func (self *AggVar) ParamBool(idx int) (bool, bool) {
	if c := self.param(idx); c != nil && c.Ty == sql.ConstBool {
		return c.Bool, true
	}
	return false, false
}

type Agg struct {
	VarList []AggVar
}

// Having phase, where we apply condition/filter for those aggregated item
type Having struct {
	Filter sql.Expr
}

// Sorting phase, will not be used to generate code inside of AWK. Typically
// this is part of the generated *bash*, since we can call *sort* command line
// to do the trick
type Sort struct {
	Asc     bool
	VarList []sql.Expr // list of variable needs to be sorted, *same* as Output
}

const (
	OutputVarValue = iota
	OutputVarWildcard
	OutputVarRowMatch
	OutputVarColMatch
)

type TableMatcher struct {
	Match   int
	Pattern string
}

type OutputVar struct {
	Type    int              // type of the output var
	Value   sql.Expr         // an expression
	Table   *TableDescriptor // which table the wildcard is bounded
	Pattern string           // alias of the output var
	Alias   string           // alias of the output var
}

type OutputVarList []*OutputVar

func (self *OutputVarList) addValue(
	v sql.Expr,
	a string,
) *OutputVar {
	*self = append(*self, &OutputVar{
		Type:  OutputVarValue,
		Value: v,
		Alias: a,
	})
	return (*self)[len(*self)-1]
}

func (self *OutputVarList) addWildcard(
	t *TableDescriptor,
	a string,
) *OutputVar {
	*self = append(*self, &OutputVar{
		Type:  OutputVarWildcard,
		Table: t,
		Alias: a,
	})
	return (*self)[len(*self)-1]
}

func (self *OutputVarList) addRowMatch(
	t *TableDescriptor,
	p string,
	a string,
) *OutputVar {
	*self = append(*self, &OutputVar{
		Type:    OutputVarRowMatch,
		Table:   t,
		Pattern: p,
		Alias:   a,
	})
	return (*self)[len(*self)-1]
}

func (self *OutputVarList) addColMatch(
	t *TableDescriptor,
	p string,
	a string,
) *OutputVar {
	*self = append(*self, &OutputVar{
		Type:    OutputVarColMatch,
		Table:   t,
		Pattern: p,
		Alias:   a,
	})
	return (*self)[len(*self)-1]
}

// Output phase, basically just print out everything. This is related to the
// selected vars
type Output struct {
	VarList  OutputVarList // output variable list
	VarSize  int           // size of variable that will be output, considering wildcard
	Wildcard bool          // whether select * shows up
	Limit    int64         // maximum allowed entries output
	Distinct bool          // whether perform distinct operation for the output
}

func (self *OutputVar) IsTableWildcard() bool {
	return self.Type == OutputVarWildcard
}

func (self *OutputVar) IsRowMatch() bool {
	return self.Type == OutputVarRowMatch
}

func (self *OutputVar) IsColMatch() bool {
	return self.Type == OutputVarColMatch
}

func (self *OutputVar) IsValue() bool {
	return self.Type == OutputVarValue
}

func (self *Output) HasTableWildcard() bool {
	for _, x := range self.VarList {
		if !x.IsValue() {
			return true
		}
	}
	return false
}

// ----------------------------------------------------------------------------
// Format phase, allowing better visualization of the dumpped data in terminal
// The format algorithm is basically layout as following. It is a structurized
// system with 3 layers, based on the customization priority, descendingly,
// ----------------------------------------------------------------------------
// 1) column[index], if applicable takes highest priority
// 2) type, if applicable kicks in
// 3) base, 1 and 2 option missed, then the base format kicks in
// ----------------------------------------------------------------------------

const (
	ColorBlack = iota
	ColorRed
	ColorGreen
	ColorYellow
	ColorBlue
	ColorMagenta
	ColorCyan
	ColorWhite
	ColorNone
)

type FormatInstruction struct {
	Ignore      bool    // whether this field is entirely ignored
	Bold        bool    // whether this field will be showed in bold font
	Italic      bool    // whether this field will be showed in italic font
	Underline   bool    // whether this field will be showed with underline
	Color       int     // color code of the field
	Index       int     // index, used only by column formatting
	StrOption   string  // string option, general option
	IntOption   int     // int value, option
	FloatOption float64 // float64 value, option
}

type Format struct {
	Title   *FormatInstruction
	Border  *FormatInstruction
	Number  *FormatInstruction
	String  *FormatInstruction
	Rest    *FormatInstruction
	Padding *FormatInstruction
	Column  []*FormatInstruction
}

func (self *Output) HasLimit() bool { return self.Limit < math.MaxInt64 }

// Planner configuration. Used to customize planner behavior
type Config struct {
	MaxColumnSize int
	MaxTableSize  int
}

type Plan struct {
	Config Config

	TableScan []*TableScan // list of table scan needed
	Join      Join         // join
	GroupBy   *GroupBy     // group by
	Agg       *Agg         // aggregation phase
	Having    *Having      // having phase
	Sort      *Sort        // delegate to other one to do the job
	Output    *Output      // output phase, must exist
	Format    *Format      // format of the plan, always valid

	// --------------------------------------------------------------------------
	// private data
	tableList []*TableDescriptor  // TableIndex is used to access the table
	alias     map[string]sql.Expr // alias table, used during symbol resolution
	prune     map[sql.Expr]bool   // contains expression used for early filter
	notPrune  []sql.Expr          // list of expression node that is not pruned
	aggExpr   []AggVar            // list of aggreation expression
}

func newPlan() *Plan {
	return &Plan{
		Config: Config{
			MaxColumnSize: defMaxColumnSize,
			MaxTableSize:  defMaxTableSize,
		},
		alias: make(map[string]sql.Expr),
		prune: make(map[sql.Expr]bool),
	}
}

func PlanCode(c *sql.Code) (*Plan, error) {
	p := newPlan()
	if err := p.plan(c.Select); err != nil {
		return nil, err
	}
	return p, nil
}

func isVTableIndex(x int) bool { return x < 0 }
func isRTableIndex(x int) bool { return x >= 0 }

func (self *TableDescriptor) IsDangling() bool {
	return self.MaxColumn == -1
}

func (self *TableDescriptor) UpdateColumnIndex(cidx int) {
	self.Column[cidx] = true
	if self.MaxColumn < cidx {
		self.MaxColumn = cidx
	}
}

func (self *TableDescriptor) SetFullColumn(v int) {
	self.MaxColumn = v
	self.FullColumn = true
}

func (self *Plan) HasJoin() bool    { return len(self.tableList) > 0 }
func (self *Plan) HasGroupBy() bool { return self.GroupBy != nil }
func (self *Plan) HasAgg() bool     { return len(self.aggExpr) > 0 }
func (self *Plan) HasHaving() bool  { return self.Having != nil }
func (self *Plan) HasSort() bool    { return self.Sort != nil }

func constListToOptions(
	constList []*sql.Const,
) Options {
	out := Options{}
	for _, c := range constList {
		switch c.Ty {
		case sql.ConstInt:
			out = append(out, c.Int)
			break

		case sql.ConstReal:
			out = append(out, c.Real)
			break

		case sql.ConstStr:
			out = append(out, c.String)
			break

		case sql.ConstBool:
			out = append(out, c.Bool)
			break

		case sql.ConstNull:
			out = append(out, nil)
			break

		default:
			break
		}
	}
	return out
}

// printing the options out
func (self *Options) Print() string {
	l := []string{}
	for _, x := range *self {
		l = append(l, fmt.Sprintf("%s", x))
	}
	return strings.Join(l, ",")
}

func (self *Plan) err(stage string, f string, args ...interface{}) error {
	msg := fmt.Sprintf(f, args...)
	return fmt.Errorf("stage(%s): %s", stage, msg)
}

func (self *Plan) isGlobalVariable(x string) bool {
	return false
}

func (self *Plan) totalTableColumnSize() int {
	cnt := 0
	for _, x := range self.tableList {
		cnt += x.MaxColumn
	}
	return cnt
}

func (self *Plan) setAllTableFullColumn() {
	for _, x := range self.tableList {
		x.SetFullColumn(self.Config.MaxColumnSize)
	}
}

func (self *Plan) wellknowncodx(n string) int {
	switch n {
	case "line":
		return 0
	case "FN", "fn":
		return ColumnIndexNF
	case "ROWNUM", "rownum":
		return ColumnIndexRowNum
	default:
		return -1
	}
}

// parse a column index into its corresponding index value. Each column index
// must be in format as $#, # represent the number, and the number should be
// positive and less than the config.MaxColumnSize
func (self *Plan) codx(c string) int {
	if len(c) == 0 {
		return -1
	}

	if x := self.wellknowncodx(c); x >= 0 {
		return x
	}

	r, _ := utf8.DecodeRuneInString(c)
	if r != '$' {
		return -1 // unknown prefix
	}

	param := c[1:]
	if x := self.wellknowncodx(param); x >= 0 {
		return x
	}

	v, err := strconv.Atoi(param)
	if err != nil {
		return -1
	}
	if v < 0 || v >= self.Config.MaxColumnSize {
		return -1
	}
	return v
}
