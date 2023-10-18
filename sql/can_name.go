package sql

import (
	"fmt"
)

const (
	CanNameFree = iota
	CanNameGlobal
	CanNameExpr
	CanNameTableColumn
	CanNameName
)

type CanName struct {
	TableIndex  int
	ColumnIndex int
	Reference   Expr   // points to another expression that this name referenced with
	Type        int    // type of can name
	Name        string // specialized usage for early stage filter
}

func (self *CanName) Set(tidx, cidx int) {
	if self.IsSettled() {
		panic("This CanName has been settled")
	}
	self.TableIndex = tidx
	self.ColumnIndex = cidx
	self.Type = CanNameTableColumn
}

func (self *CanName) SetRef(ref Expr) {
	if self.IsSettled() {
		panic("This CanName has been settled")
	}
	self.Reference = ref

	// we need to populate the TableIndex/ColumnIndex chasing down the reference
	// node, TableIndex/ColumnIndex *must always* be correct.
	r := self.Reference
	self.Type = CanNameExpr // assume it to be expression

	for {
		switch r.Type() {
		case ExprRef:
			if ref := r.(*Ref); !ref.CanName.IsReference() {
				self.TableIndex = ref.CanName.TableIndex
				self.ColumnIndex = ref.CanName.ColumnIndex
				self.Type = ref.CanName.Type
			} else {
				r = ref.CanName.Reference
				continue
			}
			break
		case ExprPrimary:
			if primary := r.(*Primary); primary.CanName.IsTableColumn() {
				self.TableIndex = primary.CanName.TableIndex
				self.ColumnIndex = primary.CanName.ColumnIndex
				self.Type = primary.CanName.Type
			}
		default:
			break
		}
		break
	}
}

func (self *CanName) SetName(
	name string,
) {
	self.Type = CanNameName
	self.Name = name
}

func (self *CanName) SetGlobal()     { self.Type = CanNameGlobal }
func (self *CanName) IsName() bool   { return self.Type == CanNameName }
func (self *CanName) IsGlobal() bool { return self.Type == CanNameGlobal }
func (self *CanName) IsExpr() bool   { return self.Type == CanNameExpr }
func (self *CanName) IsReference() bool {
	return self.IsSettled() && self.Reference != nil
}

func (self *CanName) IsTableColumn() bool {
	return self.Type == CanNameTableColumn
}
func (self *CanName) IsSettled() bool { return self.Type != CanNameFree }
func (self *CanName) IsFree() bool    { return self.Type == CanNameFree }

func (self *CanName) Reset() {
	self.Reference = nil
	self.Type = CanNameFree
}

func (self *CanName) Print() string {
	if self.IsFree() {
		return "N/A"
	}
	if self.Reference == nil {
		return fmt.Sprintf("%d:%d", self.TableIndex, self.ColumnIndex)
	} else {
		return fmt.Sprintf("ref:{%s}", PrintExpr(self.Reference))
	}
}
