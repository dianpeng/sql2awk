package cg

import (
	"fmt"
	"github.com/dianpeng/sql2awk/plan"
	"github.com/fatih/color"
	"strings"
)

// format generation code
type formatCodeGen struct {
	cg     *queryCodeGen
	writer *awkWriter
}

func (self *formatCodeGen) setWriter(w *awkWriter) {
	self.writer = w
}

func (self *formatCodeGen) mapColor(
	c int,
) color.Attribute {
	switch c {
	default:
		return color.Reset
	case plan.ColorBlack:
		return color.FgBlack
	case plan.ColorRed:
		return color.FgRed
	case plan.ColorGreen:
		return color.FgGreen
	case plan.ColorYellow:
		return color.FgYellow
	case plan.ColorBlue:
		return color.FgBlue
	case plan.ColorMagenta:
		return color.FgMagenta
	case plan.ColorCyan:
		return color.FgCyan
	case plan.ColorWhite:
		return color.FgWhite
	}
}

const termEscape = rune(0x1b)

func (self *formatCodeGen) outputAlignSize() int {
	return self.cg.formatPaddingSize()
}

func (self *formatCodeGen) lit(
	input string,
	replace string,
	quote bool,
) string {
	buf := strings.Builder{}
	if quote {
		buf.WriteString("\"")
	}
	for _, v := range input {
		if v == termEscape {
			buf.WriteString("\\033")
		} else if v == '#' {
			if quote {
				buf.WriteString("\"")
			}
			buf.WriteString(replace)
			if quote {
				buf.WriteString("\"")
			}
		} else {
			buf.WriteRune(v)
		}
	}
	if quote {
		buf.WriteString("\"")
	}
	return buf.String()
}

func (self *formatCodeGen) stylish(
	fins *plan.FormatInstruction,
	vname string,
	quote bool,
) string {
	cobj := color.New(self.mapColor(fins.Color))
	if fins.Bold {
		cobj.Add(color.Bold)
	}
	if fins.Underline {
		cobj.Add(color.Underline)
	}
	if fins.Italic {
		cobj.Add(color.Italic)
	}
	return self.lit(cobj.Sprintf("%s", "#"), vname, quote)
}

// special function that generate the title format. This will be called before
// calling the join, we all know what's the projection variables, ie output
// even without needing to perform the join
func (self *formatCodeGen) titleName(
	idx int,
	out *plan.Output,
) string {
	alias := out.VarAlias[idx]
	if alias == "" {
		return fmt.Sprintf("$%d", idx)
	} else {
		return alias
	}
}

func (self *formatCodeGen) titleFormat(
	title *plan.FormatInstruction,
	out *plan.Output,
	sep string,
) (string, string) {
	buf := []string{}
	ffmt := fmt.Sprintf("%%s%%-%ds", self.outputAlignSize())

	for idx, _ := range out.VarList {
		buf = append(
			buf,
			fmt.Sprintf(ffmt, sep, self.titleName(idx, out)),
		)
	}

	titleBar := strings.Join(buf, "")

	x := strings.Join(
		[]string{
			"\"",
			self.stylish(title, titleBar, false),
			sep,
			"\"",
		},
		"",
	)
	return x, strings.Repeat("-", len(titleBar)+1)
}

func (self *formatCodeGen) title() {
	f := self.cg.query.Format
	title := f.GetTitle()
	sep := f.GetBorderString()

	if title.Ignore {
		return
	}

	content, del := self.titleFormat(
		title,
		self.cg.query.Output,
		sep,
	)

	self.writer.Chunk(
		`
print("%[del]");
printf("%s\n", %[title]);
print("%[del]");
`,
		awkWriterCtx{
			"title": content,
			"del":   del,
		},
	)
}

func (self *formatCodeGen) genPrologue() {
	self.title()
}

func (self *formatCodeGen) genEpilogue() {
	f := self.cg.query.Format
	title := f.GetTitle()
	sep := f.GetBorderString()

	if title.Ignore {
		return
	}

	_, del := self.titleFormat(
		title,
		self.cg.query.Output,
		sep,
	)
	self.writer.Line(
		`print("%[del]");`,
		awkWriterCtx{
			"del": del,
		},
	)
}

func (self *formatCodeGen) fmtStr() string {
	return fmt.Sprintf("%%-%ds", self.outputAlignSize())
}

func generateFormatPrologue(cg *queryCodeGen) (string, *awkGlobalFromFunc) {
	w, f := newAwkWriter(
		0,
		"format_prologue",
	)
	fmtCG := &formatCodeGen{
		cg:     cg,
		writer: w,
	}
	fmtCG.genPrologue()
	return fmtCG.writer.Flush(), f
}

func generateFormatEpilogue(cg *queryCodeGen) (string, *awkGlobalFromFunc) {
	w, f := newAwkWriter(
		0,
		"format_epilogue",
	)
	fmtCG := &formatCodeGen{
		cg:     cg,
		writer: w,
	}
	fmtCG.genEpilogue()
	return fmtCG.writer.Flush(), f
}

func (self *formatCodeGen) genFormatFallbackFormat() {
	f := self.cg.query.Format
	fmtStr := self.fmtStr()

	lastResort := func() {
		self.writer.Line(
			`return "%-%[padding]s";`,
			awkWriterCtx{
				"padding": self.outputAlignSize(),
			},
		)
	}

	if f.HasTypeFormat() {
		if f.Number != nil {
			self.writer.Chunk(
				`
if (is_number(rid_0)) {
  return "%[fmt]";
}
`,
				awkWriterCtx{
					"fmt": self.stylish(f.Number, fmtStr, false),
				},
			)
		}

		if f.String != nil {
			self.writer.Chunk(
				`
if (is_string(rid_0)) {
  return "%[fmt]";
}
`,
				awkWriterCtx{
					"fmt": self.stylish(f.String, fmtStr, false),
				},
			)
		}

		if f.Rest != nil {
			self.writer.Chunk(
				`return "%[fmt]";`,
				awkWriterCtx{
					"fmt": self.stylish(f.Rest, fmtStr, false),
				},
			)
		} else {
			lastResort()
		}
	} else {
		lastResort()
	}
}

func (self *formatCodeGen) genFormatWildcardFallbackPrint() {
	f := self.cg.query.Format
	fmtStr := self.fmtStr()

	lastResort := func() {
		self.writer.Chunk(
			`
printf("%[sep]%-%[padding]s", rid_0);
return;
`,
			awkWriterCtx{
				"padding": self.outputAlignSize(),
				"sep":     self.cg.formatSep(),
			},
		)
	}

	if f.HasTypeFormat() {
		if f.Number != nil {
			self.writer.Chunk(
				`
if (is_number(rid_0)) {
  printf("%[sep]%[fmt]", rid_0)
  return;
}
`,
				awkWriterCtx{
					"fmt": self.stylish(f.Number, fmtStr, false),
					"sep": self.cg.formatSep(),
				},
			)
		}

		if f.String != nil {
			self.writer.Chunk(
				`
if (is_string(rid_0)) {
  printf("%[sep]%[fmt]", rid_0)
  return;
}
`,
				awkWriterCtx{
					"fmt": self.stylish(f.String, fmtStr, false),
					"sep": self.cg.formatSep(),
				},
			)
		}

		if f.Rest != nil {
			self.writer.Chunk(
				`
sprintf("%[sep]%[fmt]", rid_0);
return;
`,
				awkWriterCtx{
					"fmt": self.stylish(f.Rest, fmtStr, false),
					"sep": self.cg.formatSep(),
				},
			)
		} else {
			// always have one fallbback, otherwise the function fails
			lastResort()
		}
	} else {
		lastResort()
	}
}

func (self *formatCodeGen) genFormatWildcardPrintColumn() {
	f := self.cg.query.Format
	fmtStr := self.fmtStr()

	// special entry for distinct output usage. ie, user specify the freaking
	// column format and also we need to have general method to fallback if
	// we have one
	for _, y := range f.Column {
		self.writer.Chunk(
			`
if (rid_0 == %[col_idx]) {
  printf("%[sep]%[fmt]", rid_1);
  return;
}
`,
			awkWriterCtx{
				"col_idx": y.Index,
				"fmt":     self.stylish(y, fmtStr, false),
				"sep":     self.cg.formatSep(),
			},
		)
	}

	if f.HasTypeFormat() {
		self.writer.Line(
			"format_wildcard_fallback_print(rid_1);",
			nil,
		)
	} else {
		self.writer.Line(
			`printf("%[sep]%[fmt]", rid_1);`,
			awkWriterCtx{
				"fmt": fmtStr,
				"sep": self.cg.formatSep(),
			},
		)
	}
}

func generateFormatFallbackFormat(cg *queryCodeGen) (string, *awkGlobalFromFunc) {
	w, f := newAwkWriter(
		1,
		"format_fallback_format",
	)
	fmtCG := &formatCodeGen{
		cg:     cg,
		writer: w,
	}
	fmtCG.genFormatFallbackFormat()
	return fmtCG.writer.Flush(), f
}

func generateFormatWildcardFallbackPrint(cg *queryCodeGen) (string, *awkGlobalFromFunc) {
	w, f := newAwkWriter(
		1,
		"format_wildcard_fallback_print",
	)
	fmtCG := &formatCodeGen{
		cg:     cg,
		writer: w,
	}
	fmtCG.genFormatWildcardFallbackPrint()
	return fmtCG.writer.Flush(), f
}

func generateFormatWildcardPrintColumn(cg *queryCodeGen) (string, *awkGlobalFromFunc) {
	w, f := newAwkWriter(
		2,
		"format_wildcard_print_column",
	)
	fmtCG := &formatCodeGen{
		cg:     cg,
		writer: w,
	}
	fmtCG.genFormatWildcardPrintColumn()
	return fmtCG.writer.Flush(), f
}

func (self *formatCodeGen) fallbackStyle(
	loc string,
	f *plan.Format,
	fmtStr string,
) string {
	if f.HasTypeFormat() {
		return fmt.Sprintf(
			"format_fallback_format(%s)",
			loc,
		)
	} else {
		return fmtStr
	}
}

func (self *formatCodeGen) columnFormat(
	f *plan.Format,
	idx int,
	fmtStr string,
) string {
	if ff := f.GetColumn(idx); ff != nil {
		return self.stylish(ff, fmtStr, false)
	} else {
		// if there's no column formatter, try to check whether we have type related
		// formatter or not
		return self.fallbackStyle(self.writer.rid(idx), f, fmtStr)
	}
}

func (self *formatCodeGen) genNext() error {
	output := self.cg.query.Output
	if len(output.VarList) > 0 {
		f := self.cg.query.Format
		self.writer.oIndent()
		self.writer.o("printf \"")
		for idx, _ := range output.VarList {
			self.writer.o(self.cg.formatSep())
			fmtStr := self.fmtStr()
			self.writer.o(self.columnFormat(f, idx, fmtStr))
		}
		self.writer.o(self.cg.formatSep())
		self.writer.o("\",")

		self.writer.o(self.writer.ridParamStrList(self.cg.outputSize()))
		self.writer.o(";")
		self.writer.oLB()
	}
	return nil
}

func (self *formatCodeGen) genFlush() error {
	return nil
}

func (self *formatCodeGen) genDone() error {
	return nil
}
