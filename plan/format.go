package plan

import (
	"github.com/dianpeng/sql2awk/sql"
	"strings"
)

// builtin format policy
var plainFormat Format

var colorFormat Format

var defFormat = &plainFormat

const defBorder = " "

var defFormatInstruction = &FormatInstruction{
	Ignore: false,
	Color:  ColorNone,
}

var defPaddingFormatInstruction = &FormatInstruction{
	IntOption: 16,
}

var defTitleFormatInstruction = &FormatInstruction{
	Ignore: true,
}

var defBorderFormatInstruction = &FormatInstruction{
	StrOption: " ",
}

func init() {
	{
		plainFormat.Title = defTitleFormatInstruction
		plainFormat.Border = &FormatInstruction{
			Ignore:    false,
			StrOption: " ",
		}
		plainFormat.Padding = defPaddingFormatInstruction

		plainFormat.Number = nil
		plainFormat.String = nil
		plainFormat.Rest = nil
	}

	{
		colorFormat.Title = &FormatInstruction{
			Ignore: false,
			Color:  ColorBlue,
			Bold:   true,
		}
		colorFormat.Border = &FormatInstruction{
			Ignore:    true,
			StrOption: "|",
			Color:     ColorBlack,
			Bold:      true,
		}
		colorFormat.Padding = defPaddingFormatInstruction

		colorFormat.Number = &FormatInstruction{
			Color: ColorGreen,
			Bold:  true,
		}
		colorFormat.String = &FormatInstruction{
			Color:  ColorRed,
			Italic: true,
		}
		colorFormat.Rest = &FormatInstruction{
			Color: ColorNone,
		}
	}

	plainFormat.verifyfield()
	colorFormat.verifyfield()
}

func (self *Format) verifyfield() {
	if self.Title == nil {
		panic("title unset")
	}
	if self.Border == nil {
		panic("border unset")
	}
	if self.Padding == nil {
		panic("padding unset")
	}
}

func (self *Format) GetTitle() *FormatInstruction {
	if self.Title == nil {
		return defTitleFormatInstruction
	} else {
		return self.Title
	}
}

func (self *Format) GetColumn(
	idx int,
) *FormatInstruction {
	for _, x := range self.Column {
		if x.Index == idx {
			return x
		}
	}
	return nil
}

func (self *Format) GetBorderString() string {
	return self.Border.StrOption
}

func (self *Format) HasTypeFormat() bool {
	return self.Number != nil || self.String != nil || self.Rest != nil
}

func (self *Format) IsColumnFormatDefault() bool {
	return !self.HasTypeFormat() && len(self.Column) == 0
}

func (self *Plan) parseFormatInstruction(
	val *sql.Const,
) *FormatInstruction {
	switch val.Ty {
	case sql.ConstBool:
		if !val.Bool {
			return &FormatInstruction{
				Ignore: true,
			}
		} else {
			return &FormatInstruction{
				Ignore: false,
				Color:  ColorNone,
			}
		}
	case sql.ConstStr:
		f := &FormatInstruction{
			Ignore: false,
		}

		str := val.String
		rList := strings.Split(str, ";")

		for _, rr := range rList {
			switch rr {
			case "bold":
				f.Bold = true
				break
			case "italic":
				f.Italic = true
				break
			case "underline":
				f.Underline = true
				break
			case "black":
				f.Color = ColorBlack
				break
			case "red":
				f.Color = ColorRed
				break
			case "green":
				f.Color = ColorGreen
				break
			case "yellow":
				f.Color = ColorYellow
				break
			case "blue":
				f.Color = ColorBlue
				break
			case "magenta":
				f.Color = ColorMagenta
				break
			case "cyan":
				f.Color = ColorCyan
				break
			case "white":
				f.Color = ColorWhite
				break
			case "ignore":
				f.Ignore = true
				break
			default:
				// unknown, just ignore
				break
			}
		}
		return f

	default:
		return nil
	}
}

func (self *Plan) planFormat(
	s *sql.Select,
) error {
	f := s.Format
	if f == nil {
		self.Format = defFormat
		return nil
	}

	outFormat := &Format{}

	if f.Base != nil {
		if f.Base.Ty == sql.ConstStr {
			baseName := f.Base.String
			switch baseName {
			default:
				*outFormat = plainFormat
				break
			case "color":
				*outFormat = colorFormat
				break
			}
		}
	}

	if f.Title != nil {
		if opt := self.parseFormatInstruction(f.Title); opt != nil {
			outFormat.Title = opt
		}
	}
	if outFormat.Title == nil {
		outFormat.Title = defTitleFormatInstruction
	}

	if f.Padding != nil {
		if f.Base.Ty == sql.ConstInt && f.Base.Int >= int64(0) {
			outFormat.Padding = &FormatInstruction{
				IntOption: int(f.Base.Int),
			}
		}
	}
	if outFormat.Padding == nil {
		outFormat.Padding = defPaddingFormatInstruction
	}

	if f.Number != nil {
		if opt := self.parseFormatInstruction(f.Number); opt != nil {
			outFormat.Number = opt
		}
	}

	if f.String != nil {
		if opt := self.parseFormatInstruction(f.String); opt != nil {
			outFormat.String = opt
		}
	}

	if f.Rest != nil {
		if opt := self.parseFormatInstruction(f.Rest); opt != nil {
			outFormat.Rest = opt
		}
	}

	if f.Border != nil {
		if f.Border.Ty == sql.ConstStr {
			outFormat.Border = &FormatInstruction{
				StrOption: f.Border.String,
			}
		}
	}
	if outFormat.Border == nil {
		outFormat.Border = defBorderFormatInstruction
	}

	for _, col := range f.Column {
		var cFmt *FormatInstruction
		if opt := self.parseFormatInstruction(col.Value); opt != nil {
			cFmt = opt
		} else {
			tmp := *defFormatInstruction
			cFmt = &tmp
		}
		cFmt.Index = col.Index
		outFormat.Column = append(outFormat.Column, cFmt)
	}

	self.Format = outFormat
	self.Format.verifyfield()
	return nil
}
