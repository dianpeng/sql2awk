package cg

import (
	"fmt"
	gawki "github.com/benhoyt/goawk/interp"
	gawkp "github.com/benhoyt/goawk/parser"
	"github.com/dianpeng/sql2awk/plan"
	"github.com/dianpeng/sql2awk/sql"
	"github.com/stretchr/testify/assert"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"
)

const TEST_DIR = "./test/assets"

// ---------------------------------------------------------------------------
// An automatic testing/verification tools for sql2awk. We use a simple text
// file as input to help us to perform verification of sql result. The file is
// simple text format with special tag to indicate what it is
//
// ## a line comment
// @![name of section]
// @!attr1: val1
// @!attr2: val2
// @@@@@@@@@@@@@@@@@@ (start of the raw content, notes at least 3 @ should exist)
// @================= (end of the raw content, notes at least 3 = should exist)

type cookbook struct {
	filename string
	parsed   sectionList
	input    []string
	code     string
	awkProg  *gawkp.Program
	result   string
}

type section struct {
	name         string
	attr         map[string]string
	content      string
	contentstart bool // used during parsing, not ideal
	contentbuf   []string
}

type sectionList []*section

func (self *section) addAttr(k, v string) {
	self.attr[k] = v
}

func (self *section) attrAt(k string) string {
	v, ok := self.attr[k]
	if ok {
		return v
	} else {
		return ""
	}
}

func (self *sectionList) get(n string) []*section {
	out := []*section{}
	for _, x := range *self {
		if x.name == n {
			out = append(out, x)
		}
	}
	return out
}

func (self *sectionList) getOne(n string) *section {
	x := self.get(n)
	if len(x) == 0 {
		return nil
	} else {
		return x[0]
	}
}

func (self *cookbook) charAt(
	x string,
	idx int,
) rune {
	if idx >= len(x) {
		return 0
	} else {
		r, _ := utf8.DecodeRuneInString(x[idx:])
		return r
	}
}

func (self *cookbook) idAt(
	x string,
	idx int,
) (string, int) {
	cursor := idx
	leading := true
	for cursor < len(x) {
		r, sz := utf8.DecodeRuneInString(x[cursor:])
		switch r {
		case ' ', '\r', '\b', '\n', '\t', '\v':
			if !leading {
				return x[idx:cursor], cursor
			}
			break
		default:
			if r == '_' || unicode.IsDigit(r) || unicode.IsLetter(r) {
				leading = false
				break
			} else {
				return x[idx:cursor], cursor
			}
		}
		cursor += sz
	}
	if leading {
		return "", -1
	} else {
		return x[idx:cursor], cursor
	}
}

func (self *cookbook) assignAt(
	x string,
	idx int,
) int {
	cursor := idx

	for cursor < len(x) {
		r, sz := utf8.DecodeRuneInString(x[cursor:])
		switch r {
		case ' ', '\r', '\b', '\n', '\t', '\v':
			break
		case '=', ':':
			return cursor + 1

		default:
			return -1
		}
		cursor += sz
	}
	return -1
}

func (self *cookbook) anyAt(
	x string,
	idx int,
) (string, int) {
	cursor := idx
	leading := true
	for cursor < len(x) {
		r, sz := utf8.DecodeRuneInString(x[cursor:])
		switch r {
		case ' ', '\r', '\b', '\n', '\t', '\v':
			if !leading {
				return x[idx:cursor], cursor
			}
			break
		default:
			leading = false
			break
		}
		cursor += sz
	}
	return x[idx:cursor], cursor
}

func (self *cookbook) parseLineMeta(
	l string,
	curSec *section,
) (bool, error) {
	switch self.charAt(l, 2) {
	case '[':
		if curSec.name != "" {
			return false, fmt.Errorf("section name already assigned")
		}
		pos := strings.Index(l, "]")
		if pos == -1 {
			return false, fmt.Errorf("section name should be closed by ]")
		}
		curSec.name = strings.TrimSpace(l[3:pos])
		return false, nil

	default:
		key, next := self.idAt(l, 2)
		if key == "" {
			return false, fmt.Errorf("expect an id for attribute")
		}
		next = self.assignAt(l, next)
		if next == -1 {
			return false, fmt.Errorf("expect an '=' for attribute assignment")
		}
		val, _ := self.anyAt(l, next)
		if val == "" {
			return false, fmt.Errorf("expect an value for attribute")
		}
		curSec.addAttr(key, val)
		return false, nil
	}
}

func (self *cookbook) parseLineContentStart(
	l string,
	curSec *section,
) (bool, error) {
	curSec.contentstart = true
	return false, nil
}

func (self *cookbook) parseLineContentEnd(
	l string,
	curSec *section,
) (bool, error) {
	curSec.contentstart = false
	curSec.content = strings.Join(curSec.contentbuf, "\n")
	return true, nil
}

func (self *cookbook) parseLine(
	l string,
	curSec *section,
) (bool, error) {
	l = strings.TrimSpace(l)

	switch self.charAt(l, 0) {
	default:
		if curSec.contentstart {
			curSec.contentbuf = append(curSec.contentbuf, l)
		}
		return false, nil
	case '@':
		nChar := self.charAt(l, 1)
		switch nChar {
		case '!':
			return self.parseLineMeta(l, curSec)
		case '@':
			// maybe content start
			return self.parseLineContentStart(l, curSec)
		case '=':
			return self.parseLineContentEnd(l, curSec)
		default:
			break
		}
	}
	return false, nil
}

func (self *cookbook) parse(
	data string,
) error {
	curSec := &section{
		attr: make(map[string]string),
	}

	for _, l := range strings.Split(data, "\n") {
		done, err := self.parseLine(l, curSec)
		if err != nil {
			return err
		}
		if done {
			self.parsed = append(self.parsed, curSec)
			curSec = &section{
				attr: make(map[string]string),
			}
		}
	}
	return nil
}

func (self *cookbook) parseFile() error {
	f, err := os.Open(self.filename)
	if err != nil {
		return fmt.Errorf("[parsing]: %s", err)
	}
	defer f.Close()
	data, err := io.ReadAll(f)
	if err != nil {
		return fmt.Errorf("[parsing]: %s", err)
	}
	if err := self.parse(string(data)); err != nil {
		return fmt.Errorf("[parsing]: %s", err)
	}
	return nil
}

func saveToTmp(
	fn string,
	data string,
) error {
	return os.WriteFile(
		fn,
		[]byte(data),
		0644,
	)
}

func (self *cookbook) prepareTable() error {
	for _, x := range self.parsed.get("table") {
		if name := x.attrAt("name"); name != "" {
			if err := saveToTmp(
				name,
				x.content,
			); err != nil {
				return err
			} else {
				self.input = append(self.input, name)
			}
		}
	}
	return nil
}

func (self *cookbook) genAwk() error {
	yy := self.parsed.getOne("sql")
	if yy == nil {
		return fmt.Errorf("[plan]: sql is not found")
	}
	sqlCode := yy.content
	parser := sql.NewParser(sqlCode)
	c, err := parser.Parse()
	if err != nil {
		return fmt.Errorf("[plan]: %s", err)
	}
	useGoAWK := true
	if yy.attrAt("goawk") == "disable" {
		useGoAWK = false
	}
	p := plan.PlanCode(c)
	code, err := Generate(p, &Config{
		OutputSeparator: " ",
		UseGoAWK:        useGoAWK,
	})
	if err != nil {
		return fmt.Errorf("[plan]: %s", err)
	}
	self.code = code
	return nil
}

func (self *cookbook) runAwk() error {
	if verify := self.parsed.getOne("result"); verify != nil {
		prog, err := gawkp.ParseProgram(
			[]byte(self.code),
			nil,
		)
		if err != nil {
			return fmt.Errorf("[plan]: %s", err)
		}
		self.awkProg = prog
		buf := strings.Builder{}
		interp, err := gawki.New(self.awkProg)
		if err != nil {
			return err
		}
		config := &gawki.Config{
			Output: &buf,
			Args:   self.input,
		}
		_, err = interp.Execute(config)
		if err != nil {
			return err
		}
		self.result = buf.String()
	} else if save := self.parsed.getOne("save"); save != nil {
		if path := save.attrAt("path"); path != "" {
			return saveToTmp(
				path,
				self.code,
			)
		}
	}
	return nil
}

func (self *cookbook) run() error {
	if err := self.parseFile(); err != nil {
		return err
	}
	if err := self.prepareTable(); err != nil {
		return err
	}
	if err := self.genAwk(); err != nil {
		return err
	}
	if err := self.runAwk(); err != nil {
		return err
	}
	return self.verify()
}

func (self *cookbook) toOrderList(
	x string,
) [][]string {
	out := [][]string{}

	lines := strings.Split(x, "\n")
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if len(l) == 0 {
			continue
		}
		list := strings.Split(l, " ")
		out = append(out, list)
	}
	return out
}

func (self *cookbook) cmpOrderList(
	l [][]string,
	r [][]string,
	order bool,
) error {
	if order {
		sort.Slice(
			l,
			func(i, j int) bool {
				lKey := strings.Join(l[i], "@")
				rKey := strings.Join(l[j], "@")
				return lKey < rKey
			},
		)
		sort.Slice(
			r,
			func(i, j int) bool {
				lKey := strings.Join(r[i], "@")
				rKey := strings.Join(r[j], "@")
				return lKey < rKey
			},
		)
	}

	if len(l) != len(r) {
		return fmt.Errorf("lhs rhs row size not match")
	}
	sz := len(l)
	for i := 0; i < sz; i++ {
		lhs := l[i]
		rhs := r[i]
		if len(lhs) != len(rhs) {
			return fmt.Errorf("row(%d) size not match", i)
		}
		lx := strings.Join(lhs, " ")
		rx := strings.Join(rhs, " ")

		if lx != rx {
			return fmt.Errorf("row(%d) ({%s} {%s}) not match", i, lx, rx)
		}
	}
	return nil
}

func (self *cookbook) verify() error {
	if yy := self.parsed.getOne("result"); yy != nil {
		order := true
		if v := yy.attrAt("order"); v == "none" {
			order = false
		}
		lhs := self.toOrderList(yy.content)
		rhs := self.toOrderList(self.result)
		if err := self.cmpOrderList(lhs, rhs, order); err != nil {
			return fmt.Errorf("[plan]: expect{\n%s\n}, result{\n%s\n}, failed: %s",
				yy.content,
				self.result,
				err,
			)
		}
	}
	return nil
}

func TestCodeGen(t *testing.T) {
	assert := assert.New(t)
	fList, err := os.ReadDir(
		TEST_DIR,
	)
	assert.True(err == nil)
	for _, fentry := range fList {
		if fentry.IsDir() {
			continue
		}
		x := filepath.Join(TEST_DIR, fentry.Name())
		cb := &cookbook{
			filename: x,
		}
		if err := cb.run(); err != nil {
			print(fmt.Sprintf("cookbook(%s) failed: %s\n", x, err))
			assert.True(false)
		} else {
			print(fmt.Sprintf("cookbook(%s) passed\n", x))
		}
	}
}