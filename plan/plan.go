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
)

const (
	defMaxColumnSize = 60
	defMaxTableSize  = 10
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
	default:
		return "unknown"
	}
}

const (
	aggTableIndex = -1
)

type Options []interface{}

type TableDescriptor struct {
	Index      int
	Path       string
	Type       string
	Alias      string // table alias
	Options    Options
	Symbol     string       // table symbol name, used by code generation
	MaxColumn  int          // maximum column index know to us, at least one column
	Column     map[int]bool // list of column fields will be access
	FullColumn bool         // whehter require a full column dump here
}

type TableScan struct {
	Table  *TableDescriptor // which table to be scanned
	Filter sql.Expr         // early filter, if none nil
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
}

func (self *AggVar) AggName() string { return aggTypeToName(self.AggType) }

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
	Asc    bool
	Offset int
}

// Output phase, basically just print out everything. This is related to the
// selected vars
type Output struct {
	VarList  []sql.Expr
	SortList []sql.Expr // list of variable that needs to be sortted
	VarSize  int        // size of variable that will be output, considering wildcard
	Wildcard bool       // whether select * shows up
	Limit    int64      // maximum allowed entries output
	Distinct bool       // whether perform distinct operation for the output
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
	Output    *Output      // output phase, must exist
	Sort      *Sort        // delegate to other one to do the job

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

func PlanCode(c *sql.Code) *Plan {
	p := newPlan()
	p.plan(c.Select)
	return p
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

func (self *TableDescriptor) SetFullColumn() { self.FullColumn = true }

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

// parse a column index into its corresponding index value. Each column index
// must be in format as $#, # represent the number, and the number should be
// positive and less than the config.MaxColumnSize
func (self *Plan) codx(c string) int {
	if len(c) == 0 {
		return -1
	}
	r, _ := utf8.DecodeRuneInString(c)
	if r != '$' {
		return -1 // unknown prefix
	}
	v, err := strconv.Atoi(c[1:])
	if err != nil {
		return -1
	}
	if v >= self.Config.MaxColumnSize {
		return -1
	}
	return v
}
