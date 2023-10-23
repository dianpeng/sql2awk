package sql

const (
	TypeInt = iota
	TypeFloat
	TypeString
	TypeBool
)

func IsAggFunc(n string) bool {
	switch n {
	case "min", "max", "sum", "avg", "count", "histogram", "percentile":
		return true
	default:
		return false
	}
}
