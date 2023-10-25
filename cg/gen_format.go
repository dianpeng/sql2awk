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

func mapcolor(
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

func lit(
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

func stylish(
	fins *plan.FormatInstruction,
	vname string,
	quote bool,
) string {
	cobj := color.New(mapcolor(fins.Color))
	if fins.Bold {
		cobj.Add(color.Bold)
	}
	if fins.Underline {
		cobj.Add(color.Underline)
	}
	if fins.Italic {
		cobj.Add(color.Italic)
	}
	return lit(cobj.Sprintf("%s", "#"), vname, quote)
}

// special function that generate the title format. This will be called before
// calling the join, we all know what's the projection variables, ie output
// even without needing to perform the join
func (self *formatCodeGen) titleName(
	idx int,
	out *plan.Output,
) string {
	ovar := out.VarList[idx]
	if ovar.Alias == "" {
		return fmt.Sprintf("$%d", idx)
	} else {
		return ovar.Alias
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
			stylish(title, titleBar, false),
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
	title := self.cg.query.Format.GetTitle()
	if title.Ignore {
		return
	}

	if self.cg.query.Output.Wildcard {
		self.writer.Call(
			"format_wildcard_title",
			nil,
		)
	} else {
		if self.cg.query.Output.HasTableWildcard() {
			self.writer.Call(
				"format_mixed_wildcard_title",
				nil,
			)
		} else {
			self.title()
		}
	}
}

func (self *formatCodeGen) genEpilogue() {
	title := self.cg.query.Format.GetTitle()
	if title.Ignore {
		return
	}

	if self.cg.query.Output.Wildcard {
		self.writer.Call(
			"format_wildcard_title_foot",
			nil,
		)
	} else {
		if self.cg.query.Output.HasTableWildcard() {
			self.writer.Call(
				"format_mixed_wildcard_title_foot",
				nil,
			)
		} else {
			f := self.cg.query.Format
			title := f.GetTitle()
			sep := f.GetBorderString()

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
	}
}

func (self *formatCodeGen) fmtStr() string {
	return fmt.Sprintf("%%-%ds", self.outputAlignSize())
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
					"fmt": stylish(f.Number, fmtStr, false),
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
					"fmt": stylish(f.String, fmtStr, false),
				},
			)
		}

		if f.Rest != nil {
			self.writer.Chunk(
				`return "%[fmt]";`,
				awkWriterCtx{
					"fmt": stylish(f.Rest, fmtStr, false),
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
					"fmt": stylish(f.Number, fmtStr, false),
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
					"fmt": stylish(f.String, fmtStr, false),
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
					"fmt": stylish(f.Rest, fmtStr, false),
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
				"fmt":     stylish(y, fmtStr, false),
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
		return stylish(ff, fmtStr, false)
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
		self.writer.o("\\n")
		self.writer.o("\",")

		self.writer.o(self.writer.ridParamList(self.cg.outputSize()))
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

// Special builtin function for handling title and footbar of wildcard search
// which we do not know the scheme before scanning them yet. Each table will
// have 3 special global variables correspondingly
//
// 1) tbl#    : table itself, contains needed datitem
// 2) tblsize#: table's row size, used for iteration purpose
// 3) tblfnum#: table's column size, used for scanning field of each row
//
// We are just interesting in tblfnum# which can be used for us to generate
// title bar. Notes, we do not have literal way to represent the table's column
// because of the obvious reason, we do not have a schema.
func generateWildcardTitle(
	cg *queryCodeGen,
) (string, *awkGlobalFromFunc) {
	writer, f := newAwkWriter(
		0,
		"format_wildcard_title",
	)
	writer.Line("$[l, csize] = 0;", nil)

	for idx, _ := range cg.query.TableScan {
		writer.Line(`$[l, csize] += %[size]`,
			awkWriterCtx{
				"size": cg.varTableField(idx),
			},
		)
	}

	titleFmt := cg.query.Format.Title
	titleBar := cg.query.Format.GetBorderString()

	writer.Chunk(
		`
$[l, title]="";
$[g, wildcard_title_bar_sep]="";
for ($[l, i] = 0; $[l, i] < $[l, csize]; $[l, i]++) {
  $[l, title] = sprintf("%s%[sep]%-%[padding]s", $[l, title], sprintf("$%d", $[l, i]));
}

for ($[l, i] = 0; $[l, i] < length($[l, title])+1; $[l, i]++) {
  $[g, wildcard_title_bar_sep] = sprintf("%s-", $[g, wildcard_title_bar_sep]);
}

printf("%s\n", $[g, wildcard_title_bar_sep]);
printf("%[fmt]%[sep]\n", $[l, title]);
printf("%s\n", $[g, wildcard_title_bar_sep]);
`,
		awkWriterCtx{
			"sep":     titleBar,
			"padding": fmt.Sprintf("%d", cg.formatPaddingSize()),
			"fmt":     stylish(titleFmt, "%s", false),
		},
	)
	return writer.Flush(), f
}

func generateWildcardTitleFoot(
	cg *queryCodeGen,
) (string, *awkGlobalFromFunc) {
	writer, f := newAwkWriter(
		0,
		"format_wildcard_title_foot",
	)
	writer.Line(`printf("%s\n", $[g, wildcard_title_bar_sep]);`, nil)
	return writer.Flush(), f
}

func generateMixedWildcardTitle(
	cg *queryCodeGen,
) (string, *awkGlobalFromFunc) {
	writer, f := newAwkWriter(
		0,
		"format_mixed_wildcard_title",
	)
	output := cg.query.Output
	if !output.HasTableWildcard() {
		return writer.Flush(), f
	}

	writer.Line("$[l, csize] = 0;", nil)

	for idx, ovar := range output.VarList {
		switch ovar.Type {
		case plan.OutputVarWildcard, plan.OutputVarRowMatch, plan.OutputVarColMatch:
			writer.Line(`$[l, csize] += %[size];`,
				awkWriterCtx{
					"size": cg.varTableField(idx),
				},
			)
			break

		default:
			writer.Line("$[l, csize]++;", nil)
			break
		}
	}

	titleFmt := cg.query.Format.Title
	titleBar := cg.query.Format.GetBorderString()

	writer.Chunk(
		`
$[l, title]="";
$[g, mixed_wildcard_title_bar_sep]="";
for ($[l, i] = 0; $[l, i] < $[l, csize]; $[l, i]++) {
  $[l, title] = sprintf("%s%[sep]%-%[padding]s", $[l, title], sprintf("$%d", $[l, i]));
}

for ($[l, i] = 0; $[l, i] < length($[l, title])+1; $[l, i]++) {
  $[g, mixed_wildcard_title_bar_sep] = sprintf("%s-", $[g, mixed_wildcard_title_bar_sep]);
}

printf("%s\n", $[g, mixed_wildcard_title_bar_sep]);
printf("%[fmt]%[sep]\n", $[l, title]);
printf("%s\n", $[g, mixed_wildcard_title_bar_sep]);
`,
		awkWriterCtx{
			"sep":     titleBar,
			"padding": fmt.Sprintf("%d", cg.formatPaddingSize()),
			"fmt":     stylish(titleFmt, "%s", false),
		},
	)
	return writer.Flush(), f
}

func generateMixedWildcardTitleFoot(
	cg *queryCodeGen,
) (string, *awkGlobalFromFunc) {
	writer, f := newAwkWriter(
		0,
		"format_mixed_wildcard_title_foot",
	)
	writer.Line(`printf("%s\n", $[g, mixed_wildcard_title_bar_sep]);`, nil)
	return writer.Flush(), f
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
